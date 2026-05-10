/**
 * relay_code.gs  —  Synchronous GAS bridge
 *
 * Primary path  (SERVER_URL is set in Script Properties):
 *
 *   client  POST {source:'client', packets:[...]}
 *     → doPost() calls UrlFetchApp to SERVER_URL/push  (synchronous)
 *     → server dispatches, returns {ok:true, responses:[...]}
 *     → doPost returns responses inline to client
 *
 *   One HTTP round trip. No cache. No polling.
 *
 * Fallback path  (SERVER_URL missing or server unreachable):
 *
 *   doPost() caches packets under queue:server
 *   server _puller  GET ?role=server  drains queue:server
 *   server POSTs responses back  source:'server'
 *   doPost() caches under queue:client
 *   client _puller  GET ?role=client  drains queue:client
 *
 * Status / debug endpoints  (doGet ?action=...):
 *   ?action=ping    → {ok:true, ts:...}  (liveness check)
 *   ?action=status  → queue depths + pids
 *   ?action=logs    → last N debug log lines
 *   ?role=client    → long-poll fallback drain for client
 *   ?role=server    → long-poll fallback drain for server
 *
 * Script Properties (Project Settings → Script Properties):
 *   SERVER_URL   e.g. https://xxxx.ngrok-free.app
 *                Leave empty to always use fallback/cache path.
 *
 * Packet shape mirrors Python Packet.serialize():
 *   { pid, ptype, timestamp, seq, b, d, m, status, headers, ... }
 */

var props      = PropertiesService.getScriptProperties();
var SERVER_URL = (props.getProperty('SERVER_URL') || '').trim();

var CACHE_TTL     = 300;   // seconds — fallback packet TTL
var POLL_INTERVAL = 300;   // ms between cache checks in fallback doGet
var POLL_MAX      = 25000; // ms — stay under GAS 30s execution limit
var LOG_TTL       = 60;    // seconds — debug log line TTL in cache
var LOG_MAX_LINES = 100;   // lines kept in ?action=logs

var cache = CacheService.getScriptCache();


// ── Logging ───────────────────────────────────────────────────────────────────
function _log(level, msg) {
  var line = new Date().toISOString() + ' [' + level + '] ' + msg;
  Logger.log(line);
  try {
    var key = 'log:' + Date.now() + ':' + Math.random().toString(36).slice(2, 6);
    cache.put(key, line, LOG_TTL);
  } catch(e) { /* cache write failed — ignore */ }
}

function _logPacket(label, packet) {
  var b    = packet.b;
  var blen = (typeof b === 'string') ? b.length : (b ? b.length : 0);
  _log('DEBUG',
    label + ' ' +
    'pid='    + packet.pid   + ' ' +
    'ptype='  + packet.ptype + ' ' +
    'seq='    + (packet.seq !== undefined ? packet.seq : '-') + ' ' +
    'dst='    + (packet.d || packet.destination || '-') + ' ' +
    'method=' + (packet.m || packet.method || '-') + ' ' +
    'status=' + (packet.status || '-') + ' ' +
    'body='   + blen + 'B'
  );
}


// ── doPost ────────────────────────────────────────────────────────────────────
function doPost(e) {
  var startMs = Date.now();
  try {
    var body    = JSON.parse(e.postData.contents);
    var source  = body.source  || 'client';
    var packets = body.packets || [];

    _log('INFO', 'doPost source=' + source + ' packets=' + packets.length);
    for (var i = 0; i < packets.length; i++) {
      _logPacket('→ recv', packets[i]);
    }

    // ── source=client → try to forward to server synchronously ───────────────
    if (source === 'client') {
      if (SERVER_URL) {
        var result = _forwardToServer(packets);
        if (result !== null) {
          _log('INFO',
            'doPost primary path ok responses=' + result.length +
            ' elapsed=' + (Date.now() - startMs) + 'ms'
          );
          return _json({ ok: true, responses: result });
        }
        _log('WARN', 'doPost primary path failed — falling back to cache');
      }

      // Fallback: cache for server puller to pick up
      for (var i = 0; i < packets.length; i++) {
        _enqueue(packets[i], 'server');
      }
      _log('INFO',
        'doPost fallback queued=' + packets.length +
        ' elapsed=' + (Date.now() - startMs) + 'ms'
      );
      return _json({ ok: true, fallback: true, queued: packets.length });
    }

    // ── source=server → cache responses for client puller ────────────────────
    if (source === 'server') {
      for (var i = 0; i < packets.length; i++) {
        _enqueue(packets[i], 'client');
      }
      _log('INFO',
        'doPost server→client queued=' + packets.length +
        ' elapsed=' + (Date.now() - startMs) + 'ms'
      );
      return _json({ ok: true, queued: packets.length });
    }

    return _json({ ok: false, error: 'unknown source: ' + source });

  } catch (err) {
    _log('ERROR', 'doPost exception: ' + err.toString());
    return _json({ ok: false, error: err.toString() });
  }
}


// ── doGet ─────────────────────────────────────────────────────────────────────
function doGet(e) {
  var action = (e.parameter && e.parameter.action) ? e.parameter.action : '';
  var role   = (e.parameter && e.parameter.role)   ? e.parameter.role   : '';

  // ── Status/debug endpoints ────────────────────────────────────────────────
  if (action === 'ping') {
    _log('INFO', 'doGet ping');
    return _json({ ok: true, ts: Date.now(), server_url: SERVER_URL || null });
  }

  if (action === 'status') {
    return _doStatus();
  }

  if (action === 'logs') {
    return _doLogs();
  }

  // ── Fallback long-poll drain ───────────────────────────────────────────────
  // role='client' → client is waiting for cached responses (source=server)
  // role='server' → server is waiting for cached requests  (source=client)
  if (!role) {
    return _json({ ok: false, error: 'missing role or action parameter' });
  }

  _log('INFO', 'doGet poll role=' + role);

  var start     = Date.now();
  var pollCount = 0;

  while (Date.now() - start < POLL_MAX) {
    pollCount++;
    var batch = _drain(role);

    if (batch.length > 0) {
      // Sort by seq for streams so chunks arrive in order;
      // seq is 0 for plain packets so the sort is stable for them too
      batch.sort(function(a, b) {
        var seqDiff = (a.seq || 0) - (b.seq || 0);
        if (seqDiff !== 0) return seqDiff;
        return (a.pid || 0) - (b.pid || 0);
      });

      var elapsed = Date.now() - start;
      _log('INFO',
        'doGet poll returning ' + batch.length + ' packet(s) ' +
        'role=' + role + ' polls=' + pollCount + ' elapsed=' + elapsed + 'ms'
      );
      for (var i = 0; i < batch.length; i++) {
        _logPacket('← drain[' + role + ']', batch[i]);
      }
      return _json(batch);
    }

    Utilities.sleep(POLL_INTERVAL);
  }

  var elapsed = Date.now() - start;
  _log('INFO',
    'doGet poll timeout role=' + role +
    ' polls=' + pollCount + ' elapsed=' + elapsed + 'ms'
  );

  // Return 204 equivalent — empty body, client reconnects immediately
  return ContentService
    .createTextOutput('')
    .setMimeType(ContentService.MimeType.TEXT);
}


// ── Primary path: synchronous forward to server via UrlFetchApp ───────────────
function _forwardToServer(packets) {
  try {
    _log('INFO', '_forwardToServer: POST ' + SERVER_URL + '/push packets=' + packets.length);

    var response = UrlFetchApp.fetch(SERVER_URL + '/push', {
      method:             'post',
      contentType:        'application/json',
      payload:            JSON.stringify({ source: 'client', packets: packets }),
      muteHttpExceptions: true
    });

    var code = response.getResponseCode();
    var text = response.getContentText();

    _log('INFO', '_forwardToServer: response code=' + code + ' body_len=' + text.length);

    if (code !== 200) {
      _log('WARN', '_forwardToServer: server returned ' + code + ': ' + text.slice(0, 200));
      return null;
    }

    var body = JSON.parse(text);
    if (!body.ok) {
      _log('WARN', '_forwardToServer: server error: ' + (body.error || '?'));
      return null;
    }

    var responses = body.responses || [];
    _log('INFO', '_forwardToServer: got ' + responses.length + ' response(s) inline');
    for (var i = 0; i < responses.length; i++) {
      _logPacket('← server inline', responses[i]);
    }

    return responses;

  } catch (err) {
    _log('ERROR', '_forwardToServer: exception: ' + err.toString());
    return null;
  }
}


// ── Cache helpers ─────────────────────────────────────────────────────────────
function _enqueue(packet, queueName) {
  var pid = packet.pid;
  if (pid === null || pid === undefined) {
    _log('WARN', '_enqueue: packet has no pid — dropping');
    return;
  }

  var pktKey = 'pkt:' + pid;
  var qKey   = 'queue:' + queueName;

  cache.put(pktKey, JSON.stringify(packet), CACHE_TTL);

  var raw  = cache.get(qKey) || '[]';
  var pids = JSON.parse(raw);
  pids.push(pid);
  cache.put(qKey, JSON.stringify(pids), CACHE_TTL);

  _log('DEBUG',
    '_enqueue[' + queueName + ']: pid=' + pid +
    ' depth=' + pids.length
  );
}

function _drain(queueName) {
  var qKey = 'queue:' + queueName;
  var raw  = cache.get(qKey);

  if (!raw) return [];

  var pids = JSON.parse(raw);
  if (pids.length === 0) return [];

  var batch     = pids.splice(0, 50);
  var remaining = pids.length;

  if (remaining > 0) {
    cache.put(qKey, JSON.stringify(pids), CACHE_TTL);
  } else {
    cache.remove(qKey);
  }

  var keys   = batch.map(function(pid) { return 'pkt:' + pid; });
  var values = cache.getAll(keys);

  var result  = [];
  var missing = [];

  batch.forEach(function(pid) {
    var v = values['pkt:' + pid];
    if (!v) {
      missing.push(pid);
      return;
    }
    result.push(JSON.parse(v));
    cache.remove('pkt:' + pid);
  });

  if (missing.length > 0) {
    _log('WARN',
      '_drain[' + queueName + ']: ' + missing.length +
      ' packet(s) expired: ' + JSON.stringify(missing)
    );
  }

  _log('INFO',
    '_drain[' + queueName + ']: returned=' + result.length +
    ' expired=' + missing.length
  );

  return result;
}


// ── Status endpoint ───────────────────────────────────────────────────────────
function _doStatus() {
  var clientRaw  = cache.get('queue:client') || '[]';
  var serverRaw  = cache.get('queue:server') || '[]';
  var clientPids = JSON.parse(clientRaw);
  var serverPids = JSON.parse(serverRaw);

  _log('INFO', 'doGet status client=' + clientPids.length + ' server=' + serverPids.length);

  return _json({
    ok:           true,
    ts:           Date.now(),
    server_url:   SERVER_URL || null,
    queue_client: clientPids.length,
    queue_server: serverPids.length,
    client_pids:  clientPids,
    server_pids:  serverPids
  });
}


// ── Logs endpoint ─────────────────────────────────────────────────────────────
function _doLogs() {
  try {
    // Log keys are 'log:<timestamp>:<random>' — list all and return sorted
    // Note: CacheService has no list() API so we keep a rolling index instead.
    // For simplicity, just return the last LOG_MAX_LINES from Logger.getLog().
    var raw   = Logger.getLog();
    var lines = raw ? raw.split('\n').filter(Boolean) : [];
    var tail  = lines.slice(-LOG_MAX_LINES);

    return _json({ ok: true, ts: Date.now(), lines: tail });

  } catch (err) {
    return _json({ ok: false, error: err.toString() });
  }
}


// ── Utility ───────────────────────────────────────────────────────────────────
function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}