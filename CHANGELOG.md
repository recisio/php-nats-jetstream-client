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
