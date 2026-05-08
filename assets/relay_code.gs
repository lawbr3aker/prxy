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
 *
 * Packet shape mirrors Python Packet.serialize():
 *   { pid, ptype, timestamp, source, d, m, b, status, headers, ... }
 */

var CACHE_TTL     = 120;   // seconds
var POLL_INTERVAL = 500;   // ms between cache checks inside doGet
var POLL_MAX      = 25000; // ms — stay under GAS 30s execution limit

var cache = CacheService.getScriptCache();


// ── doPost ────────────────────────────────────────────────────────────────────
function doPost(e) {
  try {
    var body    = JSON.parse(e.postData.contents);
    var source  = body.source  || 'client';
    var packets = body.packets || [];

    // Packets from client → server must read them, and vice versa
    var targetQueue = (source === 'client') ? 'server' : 'client';

    for (var i = 0; i < packets.length; i++) {
      _enqueue(packets[i], targetQueue);
    }

    return _json({ ok: true, queued: packets.length, queue: targetQueue });

  } catch (err) {
    return _json({ ok: false, error: err.toString() });
  }
}


// ── doGet ─────────────────────────────────────────────────────────────────────
function doGet(e) {
  var role  = (e.parameter && e.parameter.role) ? e.parameter.role : 'client';
  var queue = role; // queue name matches the role that consumes it

  var start = Date.now();

  while (Date.now() - start < POLL_MAX) {
    var batch = _drain(queue);
    if (batch.length > 0) {
      batch.sort(function(a, b) {
        return (a.pid || 0) - (b.pid || 0);
      });
      return _json(batch);
    }
    Utilities.sleep(POLL_INTERVAL);
  }

  // Nothing ready — return empty so the puller reconnects immediately
  return ContentService
    .createTextOutput('')
    .setMimeType(ContentService.MimeType.TEXT);
}


// ── Cache helpers ─────────────────────────────────────────────────────────────
function _enqueue(packet, queueName) {
  var pid = packet.pid;
  if (pid === null || pid === undefined) return;

  cache.put('pkt:' + pid, JSON.stringify(packet), CACHE_TTL);

  var raw  = cache.get('queue:' + queueName) || '[]';
  var pids = JSON.parse(raw);
  pids.push(pid);
  cache.put('queue:' + queueName, JSON.stringify(pids), CACHE_TTL);
}

function _drain(queueName) {
  var raw = cache.get('queue:' + queueName);
  if (!raw) return [];

  var pids = JSON.parse(raw);
  if (pids.length === 0) return [];

  var batch = pids.splice(0, 50);
  cache.put('queue:' + queueName, JSON.stringify(pids), CACHE_TTL);

  var keys   = batch.map(function(pid) { return 'pkt:' + pid; });
  var values = cache.getAll(keys);

  var result = [];
  batch.forEach(function(pid) {
    var raw = values['pkt:' + pid];
    if (!raw) return;
    result.push(JSON.parse(raw));
    cache.remove('pkt:' + pid);
  });

  return result;
}


// ── Utility ───────────────────────────────────────────────────────────────────
function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
