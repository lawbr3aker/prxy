/**
 * relay_code.gs  —  Stateless GAS bridge
 *
 * Two directions, two roles:
 *
 *   client  →  doPost(source='client')  →  cache under queue:'server'
 *   server  ←  doGet(role='server')     ←  long-poll, drain queue:'server'
 *
 *   server  →  doPost(source='server')  →  cache under queue:'client'
 *   client  ←  doGet(role='client')     ←  long-poll, drain queue:'client'
 *
 * CacheService keys
 *   'queue:client'   JSON array of pids waiting for the client (responses)
 *   'queue:server'   JSON array of pids waiting for the server (requests)
 *   'pkt:<pid>'      JSON-serialised packet
 *   'log:<ts>'       Debug log entry (written by _log, read externally)
 *
 * Packet shape mirrors Python Packet.serialize():
 *   { pid, ptype, timestamp, source, d, m, b, status, headers, ... }
 */

var CACHE_TTL     = 120;   // seconds
var POLL_INTERVAL = 500;   // ms between cache checks inside doGet
var POLL_MAX      = 25000; // ms — stay under GAS 30s execution limit
var DEBUG         = true;  // set false to silence packet-level logging

var cache = CacheService.getScriptCache();


// ── Logging ───────────────────────────────────────────────────────────────────
function _log(level, msg) {
  var line = new Date().toISOString() + ' [' + level + '] ' + msg;
  Logger.log(line);
  if (DEBUG) {
    // Persist recent log lines to cache so external tools can tail them
    var key = 'log:' + Date.now();
    try { cache.put(key, line, 30); } catch(e) { /* ignore */ }
  }
}

function _logPacket(direction, packet) {
  if (!DEBUG) return;
  var b    = packet.b;
  var blen = (typeof b === 'string') ? b.length : 0;
  _log('DEBUG',
    direction + ' packet ' +
    'pid='       + packet.pid    + ' ' +
    'ptype='     + packet.ptype  + ' ' +
    'ts='        + packet.timestamp + ' ' +
    'dst='       + (packet.d || packet.destination || '-') + ' ' +
    'method='    + (packet.m || packet.method || '-') + ' ' +
    'status='    + (packet.status || '-') + ' ' +
    'body='      + blen + 'B'
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

    // Packets from client go to queue:server and vice versa
    var targetQueue = (source === 'client') ? 'server' : 'client';

    for (var i = 0; i < packets.length; i++) {
      _logPacket('→ enqueue[' + targetQueue + ']', packets[i]);
      _enqueue(packets[i], targetQueue);
    }

    var elapsed = Date.now() - startMs;
    _log('INFO',
      'doPost done queued=' + packets.length +
      ' queue=' + targetQueue +
      ' elapsed=' + elapsed + 'ms'
    );

    return _json({ ok: true, queued: packets.length, queue: targetQueue });

  } catch (err) {
    _log('ERROR', 'doPost exception: ' + err.toString());
    return _json({ ok: false, error: err.toString() });
  }
}


// ── doGet ─────────────────────────────────────────────────────────────────────
function doGet(e) {
  var role  = (e.parameter && e.parameter.role) ? e.parameter.role : 'client';
  var queue = role; // queue name matches the role that consumes it

  _log('INFO', 'doGet role=' + role + ' queue=' + queue + ' poll_max=' + POLL_MAX + 'ms');

  var start    = Date.now();
  var pollCount = 0;

  while (Date.now() - start < POLL_MAX) {
    pollCount++;
    var batch = _drain(queue);

    if (batch.length > 0) {
      batch.sort(function(a, b) {
        return (a.pid || 0) - (b.pid || 0);
      });

      var elapsed = Date.now() - start;
      _log('INFO',
        'doGet returning ' + batch.length + ' packet(s) ' +
        'role=' + role + ' polls=' + pollCount + ' elapsed=' + elapsed + 'ms'
      );
      for (var i = 0; i < batch.length; i++) {
        _logPacket('← drain[' + queue + ']', batch[i]);
      }
      return _json(batch);
    }

    Utilities.sleep(POLL_INTERVAL);
  }

  var elapsed = Date.now() - start;
  _log('INFO',
    'doGet timeout — no packets role=' + role +
    ' polls=' + pollCount + ' elapsed=' + elapsed + 'ms'
  );

  // Nothing ready — return empty so the puller reconnects immediately
  return ContentService
    .createTextOutput('')
    .setMimeType(ContentService.MimeType.TEXT);
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
  _log('DEBUG', '_enqueue: stored ' + pktKey + ' ttl=' + CACHE_TTL + 's');

  // Read-modify-write the queue list atomically within GAS's single-thread model
  var raw  = cache.get(qKey) || '[]';
  var pids = JSON.parse(raw);
  pids.push(pid);
  cache.put(qKey, JSON.stringify(pids), CACHE_TTL);

  _log('DEBUG',
    '_enqueue: queue[' + queueName + '] depth=' + pids.length +
    ' pids=' + JSON.stringify(pids.slice(-10))  // log last 10 to avoid truncation
  );
}

function _drain(queueName) {
  var qKey = 'queue:' + queueName;
  var raw  = cache.get(qKey);

  if (!raw) {
    _log('DEBUG', '_drain[' + queueName + ']: queue empty (no cache entry)');
    return [];
  }

  var pids = JSON.parse(raw);
  if (pids.length === 0) {
    _log('DEBUG', '_drain[' + queueName + ']: queue empty (zero pids)');
    return [];
  }

  // Take up to 50 pids per drain to stay within GAS URL-fetch limits
  var batch    = pids.splice(0, 50);
  var remaining = pids.length;

  // Write back the remainder (or clear the key if empty)
  if (remaining > 0) {
    cache.put(qKey, JSON.stringify(pids), CACHE_TTL);
  } else {
    cache.remove(qKey);
  }

  _log('DEBUG',
    '_drain[' + queueName + ']: draining ' + batch.length +
    ' pids remaining=' + remaining +
    ' batch=' + JSON.stringify(batch)
  );

  var keys   = batch.map(function(pid) { return 'pkt:' + pid; });
  var values = cache.getAll(keys);

  var result = [];
  var missing = [];

  batch.forEach(function(pid) {
    var raw = values['pkt:' + pid];
    if (!raw) {
      missing.push(pid);
      _log('WARN', '_drain: pkt:' + pid + ' missing from cache (expired?)');
      return;
    }
    result.push(JSON.parse(raw));
    cache.remove('pkt:' + pid);
    _log('DEBUG', '_drain: consumed pkt:' + pid);
  });

  if (missing.length > 0) {
    _log('WARN',
      '_drain[' + queueName + ']: ' + missing.length + ' packet(s) expired: ' +
      JSON.stringify(missing)
    );
  }

  _log('INFO',
    '_drain[' + queueName + ']: returned ' + result.length + ' packet(s) ' +
    '(' + missing.length + ' expired)'
  );

  return result;
}


// ── Utility ───────────────────────────────────────────────────────────────────
function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}


// ── Debug endpoint — doGet?action=logs ───────────────────────────────────────
// Calling the web app with ?action=status returns queue depths for monitoring.
// Extend doGet to route this:
//
//   function doGet(e) {
//     if (e.parameter.action === 'status') return _doStatus();
//     ...existing code...
//   }
//
function _doStatus() {
  var clientRaw  = cache.get('queue:client') || '[]';
  var serverRaw  = cache.get('queue:server') || '[]';
  var clientPids = JSON.parse(clientRaw);
  var serverPids = JSON.parse(serverRaw);

  return _json({
    queue_client: clientPids.length,
    queue_server: serverPids.length,
    client_pids:  clientPids,
    server_pids:  serverPids,
    ts:           Date.now()
  });
}
