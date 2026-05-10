var props      = PropertiesService.getScriptProperties();
var SERVER_URL = (props.getProperty('SERVER_URL') || '').trim();

var CACHE_TTL     = 300;    // seconds
var POLL_INTERVAL = 200;    // ms between cache checks in fallback doGet
var POLL_MAX      = 25000;  // ms — stay under GAS 30s limit

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


// ── Cache helpers ─────────────────────────────────────────────────────────────
function _enqueue(packet, queueName) {
  if (packet.pid === null || packet.pid === undefined) return;
  var pktKey = 'pkt:' + packet.pid + ':' + (packet.timestamp || 0);
  var qKey   = 'queue:' + queueName;
  cache.put(pktKey, JSON.stringify(packet), CACHE_TTL);
  var pids = JSON.parse(cache.get(qKey) || '[]');
  pids.push(pktKey);
  cache.put(qKey, JSON.stringify(pids), CACHE_TTL);
}

function _drain(queueName) {
  var qKey = 'queue:' + queueName;
  var raw  = cache.get(qKey);
  if (!raw) return [];
  var keys = JSON.parse(raw);
  if (!keys.length) return [];
  var batch = keys.splice(0, 50);
  if (keys.length) cache.put(qKey, JSON.stringify(keys), CACHE_TTL);
  else             cache.remove(qKey);
  var vals   = cache.getAll(batch);
  var result = [];
  for (var i = 0; i < batch.length; i++) {
    var v = vals[batch[i]];
    if (v) { result.push(JSON.parse(v)); cache.remove(batch[i]); }
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
