var props      = PropertiesService.getScriptProperties();
var SERVER_URL = (props.getProperty('SERVER_URL') || '').trim();

var CACHE_TTL     = 300;    // seconds
var POLL_INTERVAL = 200;    // ms between cache checks in fallback doGet
var POLL_MAX      = 25000;  // ms — stay under GAS 30s limit

var MAX_PAYLOAD   = 45000;  // bytes — under CacheService 100KB limit per key

var cache = CacheService.getScriptCache();

function _log(level, msg) {
  Logger.log(new Date().toISOString() + ' [' + level + '] ' + msg);
}


// ── doPost ───────────────────────────────────────────────────────────────────
function doPost(e) {
  try {
    var body    = JSON.parse(e.postData.contents);
    var source  = body.source  || 'client';
    var packets = body.packets || [];

    _log('INFO', 'doPost source=' + source + ' n=' + packets.length);

    if (source === 'client') {
      if (SERVER_URL) {
        var result = _forwardToServer(packets);
        if (result !== null) {
          _log('INFO', 'primary ok responses=' + result.length);
          return _json({ ok: true, responses: result });
        }
        _log('WARN', 'primary failed — fallback');
      }
      for (var i = 0; i < packets.length; i++) _enqueue(packets[i], 'server');
      return _json({ ok: true, fallback: true, queued: packets.length });
    }

    if (source === 'server') {
      for (var i = 0; i < packets.length; i++) _enqueue(packets[i], 'client');
      return _json({ ok: true, queued: packets.length });
    }

    return _json({ ok: false, error: 'unknown source' });
  } catch (err) {
    _log('ERROR', err.toString());
    return _json({ ok: false, error: err.toString() });
  }
}


// ── doGet ────────────────────────────────────────────────────────────────────
function doGet(e) {
  var action = (e.parameter && e.parameter.action) || '';
  var role   = (e.parameter && e.parameter.role)   || '';

  if (action === 'ping')   return _json({ ok: true, ts: Date.now(), server_url: SERVER_URL || null });
  if (action === 'status') return _doStatus();

  if (!role) return _json({ ok: false, error: 'missing role or action' });

  _log('INFO', 'poll role=' + role);

  var start = Date.now();
  while (Date.now() - start < POLL_MAX) {
    var batch = _drain(role);
    if (batch.length > 0) {
      // Sort by timestamp — ensures stream chunks delivered in order
      batch.sort(function(a, b) { return (a.timestamp || 0) - (b.timestamp || 0); });
      _log('INFO', 'poll returning n=' + batch.length + ' role=' + role);
      return _json(batch);
    }
    Utilities.sleep(POLL_INTERVAL);
  }

  // Empty response — puller reconnects immediately
  return ContentService.createTextOutput('').setMimeType(ContentService.MimeType.TEXT);
}


// ── Primary path ─────────────────────────────────────────────────────────────
function _forwardToServer(packets) {
  try {
    _log('INFO', '_forwardToServer n=' + packets.length);
    var resp = UrlFetchApp.fetch(SERVER_URL + '/', {
      method:             'post',
      contentType:        'application/json',
      payload:            JSON.stringify({ source: 'client', packets: packets }),
      muteHttpExceptions: true,
      // UrlFetchApp hard limit is ~30s; keep under GAS execution budget
    });
    var code = resp.getResponseCode();
    var text = resp.getContentText();
    _log('INFO', '_forwardToServer code=' + code);
    if (code !== 200) return null;
    var body = JSON.parse(text);
    if (!body.ok) { _log('WARN', '_forwardToServer server error: ' + (body.error || '?')); return null; }
    return body.responses || [];
  } catch (err) {
    _log('ERROR', '_forwardToServer: ' + err);
    return null;
  }
}


// ── Cache helpers (with fragmentation) ───────────────────────────────────────
function _enqueue(packet, queueName) {
  if (packet.pid === null || packet.pid === undefined) return;
  var payload = JSON.stringify(packet);
  var ts      = packet.timestamp || 0;

  // Small packet — store directly
  if (payload.length <= MAX_PAYLOAD) {
    var pktKey = 'pkt:' + packet.pid + ':' + ts;
    cache.put(pktKey, payload, CACHE_TTL);
    _pushKey(queueName, pktKey);
    return;
  }

  // Large packet — split into fragments
  var fragKeys = [];
  var index    = 0;
  while (payload.length > 0) {
    var chunk = payload.substring(0, MAX_PAYLOAD);
    payload   = payload.substring(MAX_PAYLOAD);
    var fKey  = 'pkt:' + packet.pid + ':' + ts + ':' + index;
    cache.put(fKey, chunk, CACHE_TTL);
    fragKeys.push(fKey);
    index++;
  }

  var rootKey = 'root:' + packet.pid + ':' + ts;
  cache.put(rootKey, JSON.stringify({ fragments: fragKeys, total: index }), CACHE_TTL);
  _pushKey(queueName, rootKey);
}

function _pushKey(queueName, key) {
  var qKey = 'queue:' + queueName;
  var keys = JSON.parse(cache.get(qKey) || '[]');
  keys.push(key);
  cache.put(qKey, JSON.stringify(keys), CACHE_TTL);
}

function _drain(queueName) {
  var qKey = 'queue:' + queueName;
  var raw  = cache.get(qKey);
  if (!raw) return [];
  var keys = JSON.parse(raw);
  if (!keys.length) return [];
  var batchKeys = keys.splice(0, 50);
  if (keys.length) cache.put(qKey, JSON.stringify(keys), CACHE_TTL);
  else             cache.remove(qKey);

  var result = [];
  for (var i = 0; i < batchKeys.length; i++) {
    var key = batchKeys[i];

    // Regular single‑key packet
    if (key.indexOf('root:') !== 0) {
      var val = cache.get(key);
      if (val) {
        result.push(JSON.parse(val));
        cache.remove(key);
      }
      continue;
    }

    // Fragmented packet — reassemble
    var metaVal = cache.get(key);
    if (!metaVal) continue;
    cache.remove(key);

    var meta = JSON.parse(metaVal);
    if (!meta.fragments || !meta.fragments.length) continue;

    var assembled = '';
    for (var j = 0; j < meta.fragments.length; j++) {
      var fKey = meta.fragments[j];
      var frag = cache.get(fKey);
      if (frag !== null && frag !== undefined) {
        assembled += frag;
        cache.remove(fKey);
      } else {
        // Missing fragment — drop the whole packet
        _log('WARN', 'missing fragment ' + fKey + ' for ' + key);
        assembled = null;
        break;
      }
    }
    if (assembled !== null && assembled.length > 0) {
      try {
        result.push(JSON.parse(assembled));
      } catch (e) {
        _log('ERROR', 'failed to parse reassembled packet: ' + e);
      }
    }
  }
  return result;
}

function _doStatus() {
  var c = JSON.parse(cache.get('queue:client') || '[]').length;
  var s = JSON.parse(cache.get('queue:server') || '[]').length;
  return _json({ ok: true, ts: Date.now(), server_url: SERVER_URL || null,
                 queue_client: c, queue_server: s });
}

function _json(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}