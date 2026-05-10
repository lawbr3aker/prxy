var props = PropertiesService.getScriptProperties();
var SERVER_URL = (props.getProperty('SERVER_URL') || '').trim();
var CACHE_TTL = 300;
var POLL_INTERVAL = 300;
var POLL_MAX = 25000;
var LOG_MAX_LINES = 100;

var cache = CacheService.getScriptCache();

function _log(level, msg) {
  Logger.log(new Date().toISOString() + ' [' + level + '] ' + msg);
}

function _logPacket(label, pkt) {
  var blen = (typeof pkt.body === 'string') ? pkt.body.length : (pkt.body ? pkt.body.length : 0);
  _log('DEBUG', label + ' pid=' + pkt.pid + ' seq=' + (pkt.seq||0) + ' body=' + blen + 'B');
}

function doPost(e) {
  try {
    var body = JSON.parse(e.postData.contents);
    var source = body.source || 'client';
    var packets = body.packets || [];
    _log('INFO', 'doPost source=' + source + ' packets=' + packets.length);
    for (var i = 0; i < packets.length; i++) _logPacket('→ recv', packets[i]);

    if (source === 'client') {
      if (SERVER_URL) {
        var result = _forwardToServer(packets);
        if (result !== null) return _json({ ok: true, responses: result });
        _log('WARN', 'primary failed – fallback to cache');
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

function doGet(e) {
  var action = (e.parameter && e.parameter.action) || '';
  var role = (e.parameter && e.parameter.role) || '';
  if (action === 'ping') return _json({ ok: true, ts: Date.now(), server_url: SERVER_URL || null });
  if (action === 'status') return _doStatus();
  if (!role) return _json({ ok: false, error: 'missing role' });
  _log('INFO', 'doGet poll role=' + role);
  var start = Date.now();
  var pollCount = 0;
  while (Date.now() - start < POLL_MAX) {
    pollCount++;
    var batch = _drain(role);
    if (batch.length > 0) {
      _log('INFO', 'returning ' + batch.length + ' packets role=' + role);
      return _json(batch);
    }
    Utilities.sleep(POLL_INTERVAL);
  }
  return ContentService.createTextOutput('').setMimeType(ContentService.MimeType.TEXT);
}

function _forwardToServer(packets) {
  try {
    var resp = UrlFetchApp.fetch(SERVER_URL + '/', {
      method: 'post', contentType: 'application/json',
      payload: JSON.stringify({ source: 'client', packets: packets }),
      muteHttpExceptions: true
    });
    if (resp.getResponseCode() !== 200) return null;
    var body = JSON.parse(resp.getContentText());
    if (!body.ok) return null;
    return body.responses || [];
  } catch (e) {
    _log('ERROR', '_forwardToServer: ' + e);
    return null;
  }
}

function _enqueue(packet, queueName) {
  var seq = packet.seq !== undefined ? packet.seq : 0;
  var key = 'pkt:' + packet.pid + ':' + seq;
  cache.put(key, JSON.stringify(packet), CACHE_TTL);
  var qKey = 'queue:' + queueName;
  var raw = cache.get(qKey) || '[]';
  var keys = JSON.parse(raw);
  keys.push(key);
  cache.put(qKey, JSON.stringify(keys), CACHE_TTL);
  _log('DEBUG', '_enqueue[' + queueName + '] pid=' + packet.pid + ' seq=' + seq);
}

function _drain(queueName) {
  var qKey = 'queue:' + queueName;
  var raw = cache.get(qKey);
  if (!raw) return [];
  var keys = JSON.parse(raw);
  if (!keys.length) return [];
  var batchKeys = keys.splice(0, 50);
  if (keys.length) cache.put(qKey, JSON.stringify(keys), CACHE_TTL);
  else cache.remove(qKey);
  var all = cache.getAll(batchKeys);
  var result = [];
  for (var i = 0; i < batchKeys.length; i++) {
    var v = all[batchKeys[i]];
    if (v) {
      result.push(JSON.parse(v));
      cache.remove(batchKeys[i]);
    }
  }
  _log('INFO', '_drain[' + queueName + '] returned=' + result.length);
  return result;
}

function _doStatus() {
  var cRaw = cache.get('queue:client') || '[]';
  var sRaw = cache.get('queue:server') || '[]';
  return _json({
    ok: true, ts: Date.now(), server_url: SERVER_URL || null,
    queue_client: JSON.parse(cRaw).length,
    queue_server: JSON.parse(sRaw).length
  });
}

function _json(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}