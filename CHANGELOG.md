# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

Each entry is tagged so the version impact is clear:

- `[bugfix]` - corrects wrong behavior. Bump the patch version.
- `[feature]` - adds new, backward-compatible capability. Bump the minor version.
- `[docs]` - documentation only, no code change. No release needed (or patch).
- `[bc-break]` - changes existing behavior in a way that can break callers. Bump the major version.

Note on flags: a `[bc-break]` that only corrects an evident bug is treated as a
`[bugfix]`, not a real break, even though observable behavior changes.

## [Unreleased]

Findings from a deep review against a live NATS 2.12 server (README correctness,
real-server behavior, bugs, and performance). Object Store interoperability with
the `nats` CLI, idle-connection heartbeat survival, and request-timeout recovery
were all verified working and are unchanged.

### Fixed

- `[bugfix]` The CONNECT frame now advertises the resolved client library version (from the
  installed Composer package, with a constant fallback) instead of the stale hardcoded
  `0.1.0-dev`, so server `connz`/monitoring attributes traffic to the correct version.
- `[bugfix]` Object Store `get()`/`getToCallback()` now fetch a single-chunk object with one
  Direct Get on its chunk subject, instead of creating, pulling from, and deleting a transient
  ephemeral consumer — turning the common small-object download from 4 round-trips into 1 (plus
  the metadata read). Multi-chunk objects still use the batched pull-consumer path.
- `[bugfix]` Object Store `put()` and `delete()` now run the previous-revision lookup
  concurrently with the chunk upload / tombstone publish, awaiting it only just before the
  best-effort chunk purge it feeds. Previously the lookup was a serial round-trip on the
  critical path before the first byte was written, roughly doubling small-object write
  latency. The lookup is issued at the same coroutine depth as the upload, so request
  ordering stays deterministic.
- `[bugfix]` Ephemeral push consumers (KeyValue/Object Store `watch()`, ordered consumer)
  now set an `inactive_threshold`, so the server reaps them once the subscription ends
  instead of leaking server-side consumers when a long-running app re-subscribes. An active
  subscription keeps the consumer alive; callers may override the threshold.
- `[bugfix]` `SubscriptionQueue` now bounds its polling backlog with
  `maxPendingMessagesPerSubscription` and the configured slow-consumer policy. Previously
  the connection's per-chunk drain emptied its (capped) queue into the unbounded polling
  queue, so a queue consumed slower than it was fed could grow until OOM.
- `[bugfix]` Object Store `list()` now paginates the meta-subject enumeration (via the
  STREAM.INFO `offset`) instead of reading a single page, so a bucket with more objects
  than the server's subjects-map cap is no longer silently truncated. The loop terminates
  on the first empty/duplicate page, so it is safe even against a server that ignores
  `offset`.
- `[bugfix]` `NatsHeaders::toWireBlock()` now rejects an empty/blank header name or one
  containing whitespace or a colon (previously only CR/LF were rejected, so a bad name
  silently produced a malformed/mutated block), and trims surrounding whitespace from
  values so they round-trip symmetrically with decode (which already trims).
- `[bugfix]` Object Store digest verification now compares the decoded digest *bytes*
  (tolerating missing base64url padding) with `hash_equals()`, instead of a string compare
  that spuriously rejected a byte-identical object whose metadata used unpadded base64url
  (some non-Go clients).
- `[bugfix]` KeyValue `get()`/`update()`/`delete()`/`purge()`/`getAll()` now wrap a
  malformed (non-JSON) reply in a `JetStreamException` instead of leaking a raw
  `JsonException`, consistent with `put()` and the rest of the API.
- `[bugfix]` The protocol parser now bounds an unterminated control line (no CRLF) to 1 MiB
  and raises a `ProtocolException` instead of buffering it without limit. `maxFrameSize`
  only bounded MSG/HMSG payloads (parsed after their control line completes), so a peer
  streaming bytes without a CRLF could drive the client to OOM.
- `[bugfix]` `Service::start()` is now atomic: if a subscribe fails partway, it rolls back
  the subscriptions already made and rethrows, instead of leaving the service
  half-initialized with the idempotency guard then masking a retried `start()` as a no-op.
  A separate `started` flag tracks completion.
- `[bugfix]` Microservice request observers now receive the terminal `request_end` event
  on the schema-validation rejection path too (previously only `request_start` →
  `request_error` fired), so observer spans/timers/gauges are not leaked for rejected
  (often hostile) traffic.
- `[bugfix]` `NatsClient::service()` now validates the service name
  (`^[A-Za-z0-9_-]+$`) and requires a semantic version, failing fast instead of crashing
  `start()` mid-loop or over-subscribing to discovery subjects when the name contains a
  dot/space/wildcard.
- `[bugfix]` `request()` (and every JetStream/KV/Object Store call built on it) no longer
  throws a spurious `TimeoutException` when the reply is delivered in the same event-loop
  tick the deadline fires. The wait loop now checks for completion before the deadline, so
  a reply that lands as the timeout expires is returned instead of discarded.
- `[bugfix]` Ordered-consumer gap recovery now contains a failed consumer recreate
  (pruned/deleted stream, leadership change, transient timeout) instead of throwing out of
  the shared subscription dispatch loop and aborting delivery for every other subscription
  on the connection.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now survives a
  transient `409` (`Exceeded MaxAckPending`, `Leadership Change`, `Server Shutdown`,
  `Exceeded MaxWaiting`) and keeps polling, instead of treating every non-404/408 status
  as terminal and silently exiting forever. A terminal `409 Consumer Deleted` still stops
  the loop, and finite mode is unchanged.
- `[bugfix]` `drain()` no longer busy-spins (100% CPU) or hangs when the server never
  sends the flush PONG. The flush loop now yields between empty reads so its deadline can
  fire; previously a synchronous 0-frame read starved the event loop, so the
  `TimeoutCancellation` could never fire and `drain()` never returned.
- `[bugfix]` `drain()` no longer resurrects the connection on a read failure mid-flush. A
  peer close during drain previously triggered `recoverConnection()` — reconnecting and
  re-SUBscribing the very subscriptions `drain()` had just removed (and possibly
  re-delivering messages). `processIncoming()` now skips recovery while the connection is
  `Draining` and treats the read failure as end-of-flush.
- `[bugfix]` `CredentialsParser` now parses real `nsc`-generated `.creds` files. The
  marker regex required exactly five dashes on both the BEGIN and END lines, but the
  NATS toolchain emits five dashes on BEGIN and **six** on END, so
  `CredentialsParser::fromFile()` threw `Credentials file does not contain a NATS USER
  JWT block` on essentially every genuine credentials file — making the documented
  JWT-via-`.creds` auth path unusable. Both markers now accept five-or-more dashes.
- `[bugfix]` Object Store now stores a 0-byte object with `chunks=0` and publishes no
  chunk message, matching the official Object Store layout; previously it wrote one
  empty chunk and recorded `chunks=1`. `get()` of an empty object also returns
  immediately instead of blocking until the download batch expiry waiting for a chunk
  that never arrives.
- `[bugfix]` Object Store `get()` and `getToCallback()` now return `null` for a deleted
  (tombstoned) object, consistent with a missing object and the official not-found
  semantics; the tombstone metadata remains observable via `info()`. Previously `get()`
  returned an `ObjectData` with `null` data and `getToCallback()` returned the
  `ObjectInfo`.
- `[bugfix]` Microservice handler errors no longer leak the raw exception message to the
  requester: the reply carries a generic `Internal server error` under the
  `HANDLER_ERROR` code, while the full detail stays server-side (endpoint `lastError`,
  `$SRV.STATS`, and the `request_error` observer event).
- `[bugfix]` Service `$SRV.STATS` no longer emits the non-spec `requests`/`errors`
  aliases; only the spec-compliant `num_requests`/`num_errors` remain.
- `[bugfix]` Connections now disable Nagle's algorithm (`TCP_NODELAY`). NATS is a
  small-message request/reply protocol, and Nagle combined with delayed ACKs added
  roughly 40 ms of latency per round trip; local request/reply throughput improved
  about 16x in a single-process benchmark (~22 to ~365 req/s) after this change.
- `[bugfix]` Ordered consumers (`subscribeOrderedConsumer()`) now deliver in order,
  gap-free, and without duplicates. Gap detection was based on the stream sequence,
  which is non-contiguous for a filtered consumer, so every filtered delivery looked
  like a gap; and on a gap the out-of-order message was forwarded and the expected
  sequence advanced past it, causing duplicate/out-of-order delivery and a cascading
  consumer delete+recreate storm. Detection now uses the JetStream consumer
  (delivery) sequence, the out-of-order message is discarded, and the consumer is
  recreated from the last in-order stream sequence (resuming from the next available
  message if the restart point was pruned).
- `[bugfix]` `NatsOptions` now rejects genuinely-invalid configuration at construction
  (non-positive `connectTimeoutMs`/`requestTimeoutMs`, `maxPendingMessagesPerSubscription`
  below 1, and negative reconnect/`maxPingsOut` values) with an `InvalidArgumentException`,
  instead of misbehaving later. Legitimate edge values stay valid: `pingIntervalSeconds`
  `<= 0` disables the heartbeat, `maxPingsOut` 0 is allowed, and an empty `servers` list
  falls back to the default — so this is input validation, not a breaking change.
- `[bugfix]` KeyValue keys with a leading, trailing, or consecutive dot (which produce a
  malformed `$KV.<bucket>.<key>` subject) are now rejected up front; dots, colons and
  slashes elsewhere in a key remain valid.
- `[bugfix]` Object Store `put()` now pipelines chunk publishes in bounded in-flight
  windows instead of awaiting one PubAck round-trip per chunk, so large-object uploads
  are no longer strictly round-trip-bound. PUB frames are written to the single
  connection in chunk order, so stream order (and download reassembly) is preserved.
- `[bugfix]` Single-record reads — KeyValue `get()` and Object Store `info()` (and the
  metadata read behind `get()`/`getToCallback()`) — now use the Direct Get API (served by
  any replica) instead of leader-only `STREAM.MSG.GET`, consistent with `getAll()`/`list()`.
  On clustered/replicated streams this stops concentrating reads on the stream leader. (The
  internal put/delete cleanup lookup stays on `STREAM.MSG.GET` for deterministic ordering.)
- `[bugfix]` KeyValue `getAll()` and Object Store `list()` now read the latest record
  per key/object via the Direct Get API issued concurrently, instead of N+1 sequential
  leader-only `STREAM.MSG.GET` reads. For large buckets this stops hammering the stream
  leader and collapses O(keys) serial round-trips into roughly one round-trip of
  wall-clock. (Concurrent request/reply on a single connection is covered by a new
  integration test.)
- `[bugfix]` `publish()` and `publishScheduled()` now wrap a malformed (non-JSON)
  acknowledgment in a `JetStreamException` instead of leaking a raw `JsonException`,
  consistent with the other JetStream API calls.
- `[bugfix]` Direct Get now rejects an unrecognized response (no status line and no
  `Nats-Stream`/`Nats-Sequence` headers) with a `JetStreamException` instead of
  returning a garbage body, guarding against a non-conformant server/proxy.
- `[bugfix]` KeyValue `watch()` now delivers updates through a JetStream push consumer
  (`deliver_policy=new`, ack-free) so each entry carries its `revision` (the stream
  sequence). Previously it used a plain core subscription and always reported
  `revision=null`, so a watcher could never feed an entry back into `update()`/CAS.
  Live-updates-only semantics are unchanged.
- `[feature]` `JetStreamContext::streamSequenceOf()` returns the stream sequence of a
  JetStream-delivered message (from its `$JS.ACK` reply).
- `[feature]` Object Store `watch()` now delivers updates through a JetStream push consumer
  (consistent with KeyValue `watch()`) and exposes each update's stream sequence via the new
  `ObjectInfo::$revision` field. Previously it used a plain core subscription that carried no
  sequence/revision. Live-updates-only semantics (`deliver_policy=new`, ack-free) are
  unchanged; `$revision` is `null` on `ObjectInfo`s from `get()`/`info()`/`list()`.
- `[bugfix]` `Service::stop()` now tolerates a closed/lost connection: it unsubscribes
  each endpoint best-effort and always clears its subscription state, instead of
  aborting on the first failure (which leaked the remaining subscriptions and broke a
  later `start()` restart).
- `[bugfix]` `Service::addEndpoint()` now rejects a duplicate subject with an
  `InvalidArgumentException` instead of silently overwriting the earlier endpoint and
  handler (which also under-reported them in INFO/SCHEMA/STATS).
- `[bugfix]` `Service::run()` now stops when the connection is unrecoverable (closed
  for good) and backs off interruptibly, instead of busy-spinning at ~50 Hz silently
  swallowing the error.
- `[bugfix]` The microservice discovery handler now swallows an encode/publish failure
  (e.g. invalid-UTF-8 metadata) instead of throwing out of the shared dispatch loop,
  which would abort delivery of buffered frames for other subscriptions.
- `[feature]` `NatsClient::state()` exposes the current connection state.
- `[feature]` `SubscriptionQueue::unsubscribe()` / `close()` cancel the queue's own
  subscription (convenience for `$client->unsubscribe($queue->sid)`).
- `[feature]` `AmpSocketTransport` now accepts `nats://` DSNs directly (self-normalizing
  to `tcp://`), so the transport is usable standalone, not only via the connection layer.
- `[bugfix]` Object Store downloads now use a no-ack (`ack_policy=none`) consumer. The
  read-only download previously used an explicit-ack consumer and acked each chunk; on
  a slow link an ack stalling past `ack_wait` triggered redelivery, which re-hashed a
  chunk and produced a spurious digest mismatch.
- `[bugfix]` Object Store downloads now fail on a truncated transfer (fewer chunks than
  the metadata declares) via a digest-independent completeness check, instead of
  silently returning a partial object when the metadata carries no digest.
- `[bugfix]` Object Store `watch()` now tolerates a malformed metadata payload (skips
  it) instead of throwing out of the dispatch loop, which would abort delivery of
  buffered frames for other subscriptions.
- `[bugfix]` `listStreams()` and `listConsumers()` now paginate through the JetStream
  LIST API (`offset`/`total`). Previously they read only the first page, silently
  truncating accounts with more than the server page size (256) of streams, or a
  stream with more than 256 consumers.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now keeps
  polling past routine empty windows (404/408) instead of terminating on the first
  idle gap, so a long-running worker is no longer killed by a quiet period. Terminal
  errors (e.g. 409 consumer deleted) still stop the loop, and finite mode is unchanged.
- `[bugfix]` The heartbeat watchdog now resets the outstanding-ping counter only when
  an actual PONG is received, not on any inbound bytes. Previously a server that
  stopped answering PINGs but kept trickling data (or a proxy replaying buffered data)
  never tripped `maxPingsOut`, defeating dead-link detection on busy connections.
- `[bugfix]` `drain()` now waits for the server's PONG (bounded by a deadline) before
  closing, instead of bailing on a transient partial/empty read. A larger message
  split across socket reads no longer cuts the flush short and drops in-flight
  deliveries.
- `[bugfix]` `SubscriptionQueue::fetchAll()` no longer returns early on a transient
  empty read (e.g. the heartbeat self-read briefly owning the socket) while its
  configured timeout window still has time remaining.
- `[bugfix]` `recoverConnection()` now coalesces concurrent reconnect attempts. A
  suspended ping-timer callback resuming while the read path already began recovering
  can no longer launch a second reconnect that races on the parser, state, and socket.
- `[bugfix]` The protocol parser now rejects malformed frames instead of silently
  misframing the stream: non-numeric or negative MSG/HMSG sizes and sids, and HMSG
  header bytes exceeding total bytes, raise a `ProtocolException`. A parse failure now
  resyncs past the offending bytes instead of leaving them buffered to re-throw on
  every subsequent read, and `processIncoming()` treats an unparseable stream as a
  transport failure (reconnect) rather than letting the exception escape the read loop.
- `[bugfix]` The client no longer transmits credentials in plaintext to a TLS-required
  server. When the server advertises `tls_required` (or the option/`tls://` scheme
  requires TLS) but no TLS materials were configured at connect time, the previous
  code performed a no-op "upgrade" and then wrote CONNECT (token/user/pass/JWT/sig)
  over the still-plaintext socket, hanging until the handshake deadline. The client
  now fails fast with a clear error and never writes CONNECT before TLS is active.
- `[bugfix]` A graceful peer close (socket EOF) now triggers reconnect from the read
  path. `readLine()` previously collapsed the EOF `null` into an empty string, which
  the connection treated as "no data this tick", so a server restart / idle-timeout /
  load-balancer reap left the client believing it was connected to a dead socket
  (never recovering when pings are disabled, recovering only after ~90s otherwise).
  Transports now signal EOF via a `TransportClosedException`, which `processIncoming()`
  and the heartbeat self-read escalate to `recoverConnection()`.
- `[bugfix]` `SubscriptionQueue::fetch()`, `next()` (with no/zero/negative timeout),
  and `fetchAll()` (with no timeout) no longer block the calling fiber forever on an
  idle subject against a real socket. Each now bounds its single poll with a small
  cancellation, honoring the documented non-blocking contract.
- `[bugfix]` `Service::run()` now passes its cancellation into `processIncoming()`,
  not only the outer `await()`. Previously a timed/cancelled run loop left the idle
  socket read running detached, wedging the shared connection (every later read
  short-circuited and the heartbeat stalled). The read is now torn down on cancel.
- `[bugfix]` `getStreamMessage()` no longer returns an empty payload when the stored
  body is the single character `0`. The decoded body was passed through a falsy
  fallback, so a legitimate `"0"` payload was replaced with an empty string.
- `[bugfix]` `getStreamMessage()` now preserves headers stored with the message.
  Previously the stored header block was dropped and the returned message had no
  headers.
- `[bugfix]` The heartbeat keep-alive read now delivers any application message it
  happens to read while consuming the server reply, instead of leaving it buffered
  until the next manual read. This removes a rare case where a reply could be
  delayed until its own timeout.
- `[bugfix]` Microservice endpoints now default to a shared queue group (`q`) so
  multiple instances of the same service load-balance requests, matching the NATS
  micro specification. Previously every instance handled every request, which
  duplicated side effects and work across instances. **Behavior change:** with more
  than one instance, each request is now handled by exactly one of them. Pass `null`
  or `''` as the endpoint queue group to opt out and fan out to all instances.
  (Reclassified from a breaking change to a bugfix because the previous behavior
  defeated the framework's scaling model.)

### Changed

- `[feature]` Faster Object Store downloads: object chunks are pulled in bounded
  batches rather than one request/reply round-trip per chunk, which significantly
  reduces latency for large, multi-chunk objects while keeping peak memory bounded.
  Digest verification, in-order delivery, the chunk-by-chunk `getToCallback()`
  contract, and `nats` CLI interoperability are unchanged.

### Documentation

- `[docs]` Corrected the Performance Benchmark Recipe, which previously stalled after
  roughly 50 requests because the responder consumed only one transport chunk per
  request. The recipe now drives the responder with a single continuous read loop,
  and the `processIncoming()` single-chunk semantics are spelled out.
- `[docs]` Corrected the Scheduled Publish example. It now creates the backing stream
  with `allow_msg_schedules` (and `allow_msg_ttl` when a schedule TTL is used) so the
  example runs as written; without those flags the server rejects the publish.
- `[docs]` Renamed the "Stream Message Direct Get" section to "Stream Message Get" and
  clarified that `getStreamMessage()` uses the standard stream message get API (not
  the JetStream direct-get API) and preserves the stored body and headers.
