<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Protocol\ProtocolFrame;
use IDCT\NATS\Protocol\ProtocolParser;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use Revolt\EventLoop;
use SplQueue;

use function Amp\async;
use function Amp\delay;

/**
 * Manages low-level NATS protocol connection lifecycle and frame processing.
 */
final class NatsConnection
{
    private ConnectionState $state = ConnectionState::Idle;
    private ?ServerInfo $serverInfo = null;
    private ProtocolParser $parser;
    private int $nextSid = 1;
    private int $serverCursor = 0;
    /** @var array<int, callable(NatsMessage):void> */
    private array $subscriptions = [];
    /** @var array<int, array{subject: string, queue: ?string}> */
    private array $subscriptionMeta = [];
    /** @var array<int, SplQueue<NatsMessage>> */
    private array $pendingMessages = [];
    /**
     * SIDs whose queue is currently being delivered. A subscription handler may await on the
     * connection (e.g. an ordered consumer recreating itself), which suspends the dispatch fiber
     * with readInProgress already cleared; a heartbeat tick or nested request() self-pump would then
     * re-enter the drain for the SAME sid and deliver its next message on top of the suspended one.
     * This per-sid guard makes same-sid delivery non-reentrant (FIFO preserved — the suspended loop
     * resumes and continues), while leaving OTHER sids deliverable so nested requests still complete.
     *
     * @var array<int, true>
     */
    private array $dispatchingSids = [];
    private int $outstandingPings = 0;
    private ?string $pingTimerId = null;
    private bool $drainFlushPending = false;
    /**
     * In-progress reconnect, so concurrent callers wait for it instead of starting a second one.
     *
     * @var ?DeferredFuture<void>
     */
    private ?DeferredFuture $reconnecting = null;
    /** Guards against two overlapping socket reads (user read vs heartbeat self-read). */
    private bool $readInProgress = false;

    /**
     * Creates a connection runtime with transport and protocol dependencies.
     *
     * @param NatsOptions $options Connection/runtime settings controlling handshake flags, auth, reconnect, heartbeat,
     *                             TLS, request defaults, and subscription buffering policies.
     * @param TransportInterface $transport Byte-stream transport implementation responsible for socket I/O.
     * @param ProtocolCodec $codec Encoder used to serialize NATS wire commands (CONNECT, PUB/HPUB, SUB, UNSUB, PING/PONG).
     */
    public function __construct(
        private readonly NatsOptions $options,
        private readonly TransportInterface $transport,
        private readonly ProtocolCodec $codec = new ProtocolCodec(),
    ) {
        $this->parser = new ProtocolParser();
    }

    /**
     * Returns the current connection state.
     */
    public function state(): ConnectionState
    {
        return $this->state;
    }

    /**
     * Returns server capabilities discovered during handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->serverInfo;
    }

    /**
     * Opens a transport connection and completes NATS CONNECT/PING handshake.
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        return async(function (): void {
            if ($this->state === ConnectionState::Open) {
                return;
            }

            try {
                $this->connectOnce();
            } catch (\Throwable $e) {
                if ($this->options->reconnectEnabled && $this->options->maxReconnectAttempts > 0) {
                    $this->recoverConnection();

                    return;
                }

                $this->state = ConnectionState::Closed;
                throw new ConnectionException($e->getMessage(), (int) $e->getCode(), $e);
            }
        });
    }

    /**
     * Closes the transport and marks the runtime as closed.
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        return async(function (): void {
            $this->cancelPingTimer();
            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;
        });
    }

    /**
     * Gracefully drains all subscriptions, flushes pending messages, then closes.
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        return async(function (): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->state = ConnectionState::Draining;
            $this->cancelPingTimer();

            // Send UNSUB for all active subscriptions so no new messages arrive.
            foreach (array_keys($this->subscriptionMeta) as $sid) {
                $this->transport->write($this->codec->encodeUnsubscribe($sid))->await();
            }

            // Flush in-flight deliveries already emitted by the server before closing.
            $this->drainFlushPending = true;
            $this->transport->write($this->codec->encodePing())->await();

            // Read until the server's PONG confirms the flush (handleFrame clears drainFlushPending),
            // bounded by a deadline so a slow/wedged server cannot hang drain() forever. A partial
            // chunk (0 complete frames yet) must NOT end the flush early — only the PONG or the
            // deadline does.
            $flushCancellation = new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000));
            try {
                while (!$flushCancellation->isRequested()) {
                    $frames = $this->processIncoming($flushCancellation)->await();

                    if (!$this->drainFlushPending) {
                        // The server's PONG arrived (handleFrame cleared the flag): flush complete.
                        break;
                    }

                    if ($frames === 0) {
                        // No complete frame this read. Yield so the event loop advances and the
                        // deadline can fire — processIncoming() returns 0 synchronously on an empty
                        // read, so without this the loop would busy-spin and starve the timer forever.
                        delay(0.001, cancellation: $flushCancellation);
                    }
                }
            } catch (CancelledException) {
                // Flush deadline reached; close with whatever was delivered.
            } catch (\Throwable) {
                // A fatal frame (e.g. a server -ERR) surfaced mid-flush. Stop flushing and fall through
                // to the cleanup below so drain() still closes the socket and clears state rather than
                // leaving the connection wedged in Draining with the socket open.
            }

            // Drain any remaining buffered messages to callbacks.
            $this->drainAllPending();

            // Clear subscription state.
            $this->subscriptions = [];
            $this->subscriptionMeta = [];
            $this->pendingMessages = [];
            $this->drainFlushPending = false;

            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;
        });
    }

    /**
     * Publishes payload bytes to the given subject.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return async(function () use ($subject, $payload, $replyTo): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->validateSubject($subject);
            if ($replyTo !== null) {
                $this->validateSubject($replyTo);
            }
            $this->enforceMaxPayload(strlen($payload));

            try {
                $this->transport->write($this->codec->encodePublish($subject, $payload, $replyTo))->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($this->codec->encodePublish($subject, $payload, $replyTo))->await();
            }
        });
    }

    /**
     * Publishes payload bytes with NATS headers to the given subject.
     *
     * @param array<string,string> $headers
     * @return Future<void>
     */
    public function publishWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?string $replyTo = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $replyTo): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->validateSubject($subject);
            if ($replyTo !== null) {
                $this->validateSubject($replyTo);
            }
            // Build (and CR/LF-validate) the header wire block once, then reuse it for sizing and for
            // each write attempt, instead of re-running toWireBlock() per call.
            $headerBlock = NatsHeaders::toWireBlock($headers);
            $this->enforceMaxPayload(strlen($headerBlock) + strlen($payload));

            try {
                $this->transport->write($this->codec->encodeHeaderPublishBlock($subject, $payload, $headerBlock, $replyTo))->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($this->codec->encodeHeaderPublishBlock($subject, $payload, $headerBlock, $replyTo))->await();
            }
        });
    }

    /**
     * Registers a subscription callback and sends a SUB command.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        return async(function () use ($subject, $handler, $queue): int {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->validateSubject($subject, allowWildcards: true);
            if ($queue !== null) {
                $this->validateQueueGroup($queue);
            }
            $sid = $this->nextSid++;
            $this->subscriptions[$sid] = $handler;
            $this->subscriptionMeta[$sid] = ['subject' => $subject, 'queue' => $queue];
            $this->pendingMessages[$sid] = new SplQueue();

            $this->transport->write($this->codec->encodeSubscribe($subject, $sid, $queue))->await();

            return $sid;
        });
    }

    /**
     * Removes a subscription callback and sends an UNSUB command.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return async(function () use ($sid, $maxMessages): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->transport->write($this->codec->encodeUnsubscribe($sid, $maxMessages))->await();
            $this->dropSubscriptionState($sid);
        });
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks.
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read,
     *                                        so a timed-out caller does not orphan an in-flight read.
     * @return Future<int>
     *
     * @phpstan-impure Mutates connection state (e.g. clears drainFlushPending / outstandingPings via
     *                 handled frames), so callers must not assume remembered property values persist.
     */
    public function processIncoming(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): int {
            if ($this->state !== ConnectionState::Open && $this->state !== ConnectionState::Draining) {
                throw new ConnectionException('Connection is not open');
            }

            if ($this->readInProgress) {
                // A concurrent read (e.g. the heartbeat timer) owns the socket; avoid a second
                // overlapping read which the transport would reject with a pending-read error.
                return 0;
            }

            $this->readInProgress = true;

            try {
                $chunk = $this->transport->readLine($cancellation)->await();
            } catch (CancelledException $cancelledException) {
                throw $cancelledException;
            } catch (\Throwable) {
                // During drain() a read failure means the flush is finished, not a fault to recover
                // from: recovering would reconnect and re-SUBscribe the very subscriptions drain()
                // just UNSUBbed (and could re-deliver). Treat it as end-of-flush instead.
                if ($this->state !== ConnectionState::Draining) {
                    $this->recoverConnection();
                }

                return 0;
            } finally {
                $this->readInProgress = false;
            }

            if ($chunk === '') {
                return 0;
            }

            try {
                $frames = $this->parser->push($chunk);
            } catch (ProtocolException) {
                // An unparseable/corrupt stream is a transport-level failure: reconnect rather than
                // letting the exception escape the caller's processing loop. The parser has already
                // resynced past the offending bytes, so a recovery-disabled retry will not re-throw.
                $this->recoverConnection();

                return 0;
            }

            foreach ($frames as $frame) {
                $this->handleFrame($frame);
            }

            // Note: the outstanding-ping counter is reset only when an actual PONG is handled (see
            // handleFrame), not on any inbound bytes — otherwise a server that stops answering PINGs
            // but still trickles data would never trip maxPingsOut and the watchdog could not escalate.

            // Drain buffered deliveries after each chunk to preserve wire-order delivery.
            $this->drainAllPending();

            return count($frames);
        });
    }

    /**
     * Sends a request and awaits the first response on an auto-generated inbox subject.
     *
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function request(
        string $subject,
        string $payload,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $timeoutMs, $cancellation): NatsMessage {
            $this->validateSubject($subject);

            return $this->requestInternal($subject, $payload, null, $timeoutMs, $cancellation);
        });
    }

    /**
     * Sends a request with headers and awaits the first response.
     *
     * @param array<string,string> $headers
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function requestWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $timeoutMs, $cancellation): NatsMessage {
            $this->validateSubject($subject);

            return $this->requestInternal($subject, $payload, $headers, $timeoutMs, $cancellation);
        });
    }

    /**
     * Executes request/reply flow using plain publish or header publish variants.
     *
     * @param array<string,string>|null $headers
     */
    private function requestInternal(
        string $subject,
        string $payload,
        ?array $headers,
        ?int $timeoutMs,
        ?Cancellation $cancellation,
    ): NatsMessage {
        if ($this->state !== ConnectionState::Open) {
            throw new ConnectionException('Connection is not open');
        }

        $inbox = Inbox::generate($this->options->inboxPrefix);
        /** @var DeferredFuture<NatsMessage> $deferred */
        $deferred = new DeferredFuture();
        // Set by the handler when the reply is delivered. The wait loop checks this rather than
        // $deferred->isComplete() so a reply delivered in the same tick the deadline fires is
        // returned instead of being discarded as a spurious timeout.
        $replyReceived = false;

        $sid = $this->subscribe($inbox, static function (NatsMessage $message) use ($deferred, &$replyReceived): void {
            if (!$deferred->isComplete()) {
                $deferred->complete($message);
                $replyReceived = true;
            }
        })->await();

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $inbox)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $inbox)->await();
            }

            $deadlineMs = $timeoutMs ?? $this->options->requestTimeoutMs;
            if ($deadlineMs <= 0) {
                throw new TimeoutException('Request timeout must be greater than zero');
            }

            $timeoutCancellation = new TimeoutCancellation($deadlineMs / 1000);
            $waitCancellation = $cancellation === null
                ? $timeoutCancellation
                : new CompositeCancellation($cancellation, $timeoutCancellation);

            while (true) {
                // Completion is checked BEFORE the deadline so a reply delivered in the same tick the
                // deadline fires (by this loop's read or a concurrent heartbeat read) is returned
                // rather than discarded as a spurious timeout.
                if ($replyReceived) {
                    break;
                }

                if ($waitCancellation->isRequested()) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw new CancelledException();
                    }

                    throw new TimeoutException('Request timed out for subject ' . $subject);
                }

                try {
                    $frames = $this->processIncoming($waitCancellation)->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // The deadline fired during the read. Loop once more: the top-of-loop check
                    // returns the reply if it was delivered in the same tick, otherwise the deadline
                    // check there throws the timeout.
                    continue;
                }

                if ($frames === 0) {
                    // No data available from transport; yield to avoid a tight spin.
                    delay(0.001);
                }
            }

            $response = $deferred->getFuture()->await();

            if ($this->isNoRespondersStatus($response)) {
                throw new NatsException('No responders for subject ' . $subject);
            }

            return $response;
        } finally {
            $this->cleanupRequestSubscription($sid);
        }
    }

    /**
     * Checks whether a response message carries a 503 No Responders status.
     */
    private function isNoRespondersStatus(NatsMessage $message): bool
    {
        if ($message->rawHeaders === null) {
            return false;
        }

        $firstLine = explode("\r\n", $message->rawHeaders, 2)[0];
        if ($firstLine === '') {
            return false;
        }

        // Status line format: "NATS/1.0 503" or "NATS/1.0 503 No Responders".
        return (bool) preg_match('/^NATS\/1\.0\s+503\b/', $firstLine);
    }

    /**
     * Determines whether the connection must be upgraded to TLS, based on the configured option,
     * the server URL scheme, and the server's advertised TLS requirement.
     */
    private function requiresTls(string $server, ServerInfo $serverInfo): bool
    {
        return $this->options->tlsRequired
            || str_starts_with($server, 'tls://')
            || $serverInfo->tlsRequired;
    }

    /**
     * Normalizes NATS DSN scheme to the transport-compatible scheme.
     */
    private function normalizeDsn(string $server): string
    {
        $normalized = preg_replace('#^nats://#', 'tcp://', $server);
        if ($normalized === null) {
            throw new ConnectionException('Invalid server DSN');
        }

        return $normalized;
    }

    /**
     * Establishes a fresh connection against the next available server.
     */
    private function connectOnce(): void
    {
        $this->state = ConnectionState::Connecting;

        $server = $this->nextServer();
        $dsn = $this->normalizeDsn($server);
        $this->transport->connect($dsn, $this->options->connectTimeoutMs)->await();

        $this->serverInfo = $this->awaitServerInfo();

        // Standard NATS TLS upgrade: after the plaintext INFO, upgrade the socket to TLS (unless the
        // handshake-first path already negotiated TLS during connect()).
        if (!$this->options->tlsHandshakeFirst && $this->requiresTls($server, $this->serverInfo)) {
            $this->transport->upgradeTls()->await();
        }

        // Never write CONNECT (which carries credentials) over a socket that is still plaintext when
        // TLS is required — regardless of which path was meant to establish it. This guard runs for the
        // handshake-first path too, so a misconfiguration (tlsHandshakeFirst=true but no TLS materials
        // or a nats:// DSN, while the server's INFO advertises tls_required) fails fast instead of
        // leaking credentials in cleartext.
        if ($this->requiresTls($server, $this->serverInfo)
            && $this->transport instanceof TlsAwareTransportInterface
            && !$this->transport->tlsActive()
        ) {
            throw new ConnectionException(
                'Server requires TLS but the TLS handshake was not established; '
                . 'configure TLS materials (NatsOptions tlsRequired / tlsCaFile / tlsCertFile) for this connection',
            );
        }

        $this->transport->write($this->codec->encodeConnect($this->options, $this->serverInfo->nonce))->await();
        $this->transport->write($this->codec->encodePing())->await();

        $this->awaitInitialPong();
        // Reset parser state after handshake to avoid carrying partial bootstrap chunks.
        $this->parser = new ProtocolParser();
        $this->state = ConnectionState::Open;
        $this->startPingTimer();
    }

    /**
     * Returns the next server endpoint using round-robin rotation.
     */
    private function nextServer(): string
    {
        $servers = $this->options->servers;
        if ($servers === []) {
            return NatsOptions::DEFAULT_SERVER;
        }

        $index = $this->serverCursor % count($servers);
        $this->serverCursor++;

        return $servers[$index];
    }

    /**
     * Reconnects using retry policy and restores subscription state.
     *
     * Concurrent callers are coalesced: while one reconnect is running, others (e.g. a ping-timer
     * callback resuming after its write while the read path already began recovering) await the same
     * attempt and share its outcome, rather than racing on the parser, state, and socket.
     */
    private function recoverConnection(): void
    {
        $inProgress = $this->reconnecting;
        if ($inProgress !== null) {
            $inProgress->getFuture()->await();

            return;
        }

        $deferred = new DeferredFuture();
        // Suppress unhandled-error reporting for the no-waiter case; awaiting callers still receive
        // the error from await().
        $deferred->getFuture()->ignore();
        $this->reconnecting = $deferred;

        try {
            $this->performRecovery();
            $deferred->complete();
        } catch (\Throwable $e) {
            $deferred->error($e);

            throw $e;
        } finally {
            $this->reconnecting = null;
        }

        // Deliver any messages buffered during subscription replay now that recovery has finished and
        // we are OUT of the critical section: `reconnecting` is cleared, so a callback that publishes
        // and hits a write failure starts a fresh recovery instead of deadlocking on the in-progress
        // one, and the per-sid dispatch guard keeps it non-reentrant. (Only reached on success; the
        // catch above rethrows on failure.)
        $this->drainAllPending();
    }

    /**
     * Performs the actual reconnect + subscription replay, serialized by {@see recoverConnection()}.
     */
    private function performRecovery(): void
    {
        if (!$this->options->reconnectEnabled) {
            $this->state = ConnectionState::Closed;
            throw new ConnectionException('Reconnect is disabled');
        }

        $this->cancelPingTimer();

        $maxAttempts = max(1, $this->options->maxReconnectAttempts);
        $lastError = null;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $this->transport->close()->await();
            } catch (\Throwable) {
                // Ignore close failures during reconnect transitions.
            }

            try {
                $this->connectOnce();
                $this->resubscribeAll();

                return;
            } catch (\Throwable $e) {
                $lastError = $e;
                $delayMs = $this->backoffDelayMs($attempt);
                delay($delayMs / 1000);
            }
        }

        $this->state = ConnectionState::Closed;
        throw new ConnectionException(
            'Reconnect attempts exhausted',
            0,
            $lastError,
        );
    }

    /**
     * Replays existing SUB registrations after a reconnect.
     */
    private function resubscribeAll(): void
    {
        foreach ($this->subscriptionMeta as $sid => $meta) {
            $this->transport->write($this->codec->encodeSubscribe($meta['subject'], $sid, $meta['queue']))->await();
            $this->drainImmediateServerFrames();
        }
    }

    /**
     * Polls for any immediate frames emitted by the server after a protocol write.
     *
     * This is primarily used during reconnect subscription replay so prompt `-ERR`
     * responses do not leave the connection open with silently rejected subscriptions.
     *
     * It deliberately does NOT drain message deliveries to user callbacks: this runs inside the
     * reconnect critical section (with state already Open and `reconnecting` set), and a callback that
     * publishes and hits a write failure would re-enter recoverConnection(), await the in-progress
     * reconnect deferred, and deadlock. Message frames captured here are buffered via handleFrame() and
     * delivered by the normal processIncoming()/heartbeat drain once recovery has completed.
     */
    private function drainImmediateServerFrames(): void
    {
        $maxPolls = 16;
        $pollTimeoutMs = 5;

        for ($poll = 0; $poll < $maxPolls; $poll++) {
            try {
                $chunk = $this->transport->readLine(new TimeoutCancellation($pollTimeoutMs / 1000))->await();
            } catch (CancelledException) {
                return;
            }

            if ($chunk === '') {
                return;
            }

            $frames = $this->parser->push($chunk);
            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Ok) {
                    continue;
                }

                $this->handleFrame($frame);
            }
        }
    }

    /**
     * Computes reconnect delay with exponential backoff, capped at reconnectMaxDelayMs.
     */
    private function backoffDelayMs(int $attempt): int
    {
        $base = max(1, $this->options->reconnectDelayMs);
        $exponential = (int) ($base * (2 ** ($attempt - 1)));
        $capped = min($exponential, max($base, $this->options->reconnectMaxDelayMs));
        $jitter = $this->options->reconnectJitterMs > 0 ? random_int(0, $this->options->reconnectJitterMs) : 0;

        return $capped + $jitter;
    }

    /**
     * Waits for initial PONG while handling expected intermediary control lines.
     */
    private function awaitInitialPong(): void
    {
        $deadline = $this->handshakeDeadline();
        $remainingPolls = $this->handshakePollBudget();

        while ($remainingPolls-- > 0 && microtime(true) < $deadline) {
            $chunk = $this->readHandshakeChunk($deadline);
            if ($chunk === null || $chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Ok) {
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                    $this->serverInfo = $this->decodeServerInfoPayload($frame->infoPayload);

                    continue;
                }

                if ($frame->type === ProtocolFrameType::Pong) {
                    return;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw new ConnectionException('Server error during connect: ' . ($frame->error ?? 'unknown'));
                }
            }
        }

        throw new ConnectionException('Expected PONG after CONNECT');
    }

    /**
     * Waits for and parses the initial INFO frame sent by the server.
     */
    private function awaitServerInfo(): ServerInfo
    {
        $deadline = $this->handshakeDeadline();
        $remainingPolls = $this->handshakePollBudget();

        while ($remainingPolls-- > 0 && microtime(true) < $deadline) {
            $chunk = $this->readHandshakeChunk($deadline);
            if ($chunk === null || $chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                    /** @var array<string,mixed> $data */
                    $data = json_decode($frame->infoPayload, true, 512, JSON_THROW_ON_ERROR);

                    return ServerInfo::fromInfoPayload($data);
                }

                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();

                    continue;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw new ConnectionException('Server error during connect: ' . ($frame->error ?? 'unknown'));
                }
            }
        }

        throw new ConnectionException('Expected INFO during connect');
    }

    /**
     * Returns the absolute handshake deadline based on connect timeout.
     */
    private function handshakeDeadline(): float
    {
        $timeoutSeconds = max(0.001, $this->options->connectTimeoutMs / 1000);

        return microtime(true) + $timeoutSeconds;
    }

    /**
     * Bounds handshake polling for transports that may return empty chunks immediately.
     */
    private function handshakePollBudget(): int
    {
        return max(16, (int) ceil(max(1, $this->options->connectTimeoutMs) / 10));
    }

    /**
     * Reads the next handshake chunk within the remaining timeout budget.
     */
    private function readHandshakeChunk(float $deadline): ?string
    {
        $remainingMs = (int) ceil(($deadline - microtime(true)) * 1000);
        if ($remainingMs <= 0) {
            return null;
        }

        $sliceMs = min($remainingMs, 50);

        try {
            return $this->transport->readLine(new TimeoutCancellation(max(1, $sliceMs) / 1000))->await();
        } catch (CancelledException) {
            return null;
        }
    }

    /**
     * Handles non-message frames immediately and queues message frames for delivery.
     */
    private function handleFrame(ProtocolFrame $frame): void
    {
        if ($frame->type === ProtocolFrameType::Ping) {
            $this->transport->write($this->codec->encodePong())->await();

            return;
        }

        if ($frame->type === ProtocolFrameType::Pong) {
            $this->outstandingPings = 0;
            $this->drainFlushPending = false;

            return;
        }

        if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
            $this->serverInfo = $this->decodeServerInfoPayload($frame->infoPayload);

            return;
        }

        if ($frame->type === ProtocolFrameType::Err) {
            $error = $frame->error ?? 'unknown';
            if ($this->isRecoverableServerError($error)) {
                return;
            }

            throw new ConnectionException('Server sent error frame: ' . $error);
        }

        if ($frame->type === ProtocolFrameType::Msg || $frame->type === ProtocolFrameType::HMsg) {
            $sid = $frame->sid;
            if ($sid === null || !isset($this->subscriptions[$sid])) {
                return;
            }

            [$rawHeaders, $payload] = $this->extractHeadersAndPayload($frame);
            $message = new NatsMessage(
                subject: $frame->subject ?? '',
                sid: $sid,
                replyTo: $frame->replyTo,
                payload: $payload,
                rawHeaders: $rawHeaders,
            );

            $this->enqueueMessage($sid, $message);
        }
    }

    /**
     * Splits HMSG combined data into raw headers and payload body bytes.
     *
     * @return array{0: ?string, 1: string}
     */
    private function extractHeadersAndPayload(ProtocolFrame $frame): array
    {
        $payload = $frame->payload ?? '';

        if ($frame->type !== ProtocolFrameType::HMsg || $frame->headerBytes === null || $frame->headerBytes <= 0) {
            return [null, $payload];
        }

        if ($frame->headerBytes > strlen($payload)) {
            throw new ProtocolException('Malformed HMSG frame: header bytes exceed payload length');
        }

        // Header bytes include only the wire header block; remainder is message body.
        $headerBytes = $frame->headerBytes;
        $headers = substr($payload, 0, $headerBytes);
        $body = substr($payload, $headerBytes);

        return [$headers, $body];
    }

    /**
     * Adds a message to a subscription queue and applies slow-consumer policy when full.
     */
    private function enqueueMessage(int $sid, NatsMessage $message): void
    {
        if (!isset($this->pendingMessages[$sid])) {
            $this->pendingMessages[$sid] = new SplQueue();
        }

        $queue = $this->pendingMessages[$sid];
        $limit = max(1, $this->options->maxPendingMessagesPerSubscription);

        if ($queue->count() >= $limit) {
            if ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $queue->dequeue();
            } elseif ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                return;
            } else {
                throw new ConnectionException('Subscription queue overflow for sid ' . $sid);
            }
        }

        $queue->enqueue($message);
    }

    /**
     * Drains all queued subscription messages in SID order.
     */
    private function drainAllPending(): void
    {
        foreach (array_keys($this->pendingMessages) as $sid) {
            $this->drainPendingForSid($sid);
        }
    }

    /**
     * Validates that payload size does not exceed server max_payload.
     */
    private function enforceMaxPayload(int $totalBytes): void
    {
        if ($this->serverInfo === null) {
            return;
        }

        $max = $this->serverInfo->maxPayload;
        if ($max > 0 && $totalBytes > $max) {
            throw new ProtocolException(sprintf(
                'Payload size %d exceeds server max_payload of %d',
                $totalBytes,
                $max,
            ));
        }
    }

    /**
     * Parses an INFO payload JSON fragment into ServerInfo.
     */
    private function decodeServerInfoPayload(string $infoPayload): ServerInfo
    {
        /** @var array<string,mixed> $data */
        $data = json_decode($infoPayload, true, 512, JSON_THROW_ON_ERROR);

        return ServerInfo::fromInfoPayload($data);
    }

    /**
     * Returns true when a server -ERR is documented as connection-nonfatal.
     */
    private function isRecoverableServerError(string $error): bool
    {
        $normalized = strtolower(trim($error, " '\t\r\n\0\x0B"));

        if ($normalized === 'invalid subject') {
            return true;
        }

        return str_starts_with($normalized, 'permissions violation for subscription to ')
            || str_starts_with($normalized, 'permissions violation for publish to ');
    }

    /**
     * Starts the periodic ping timer based on configured interval.
     */
    private function startPingTimer(): void
    {
        $this->cancelPingTimer();
        $this->outstandingPings = 0;

        $intervalSeconds = $this->options->pingIntervalSeconds;
        if ($intervalSeconds <= 0) {
            return;
        }

        $this->pingTimerId = EventLoop::repeat($intervalSeconds, function (): void {
            if ($this->state !== ConnectionState::Open) {
                $this->cancelPingTimer();

                return;
            }

            $this->outstandingPings++;

            if ($this->outstandingPings > $this->options->maxPingsOut) {
                $this->cancelPingTimer();

                try {
                    $this->recoverConnection();
                } catch (\Throwable) {
                    $this->state = ConnectionState::Closed;
                }

                return;
            }

            try {
                $this->transport->write($this->codec->encodePing())->await();
            } catch (\Throwable) {
                $this->cancelPingTimer();

                try {
                    $this->recoverConnection();
                } catch (\Throwable) {
                    $this->state = ConnectionState::Closed;
                }

                return;
            }

            // Consume the server PONG ourselves so liveness detection does not depend on the
            // application actively calling processIncoming(). If a user read is already running,
            // it will consume the PONG instead and reset the counter.
            $this->consumeHeartbeatResponse();
        });
    }

    /**
     * Performs a short, bounded read to consume the heartbeat PONG (and any other control frames)
     * without colliding with an in-flight user read. Any message frames captured during this read
     * are delivered immediately via drainAllPending(); control frames (PONG/PING/INFO) are handled
     * inline.
     */
    private function consumeHeartbeatResponse(): void
    {
        if ($this->readInProgress) {
            return;
        }

        $timeoutSeconds = min(2.0, max(0.05, (float) $this->options->pingIntervalSeconds));

        $this->readInProgress = true;

        $closed = false;
        try {
            $chunk = $this->transport->readLine(new TimeoutCancellation($timeoutSeconds))->await();
        } catch (TransportClosedException) {
            // The peer closed the socket during the heartbeat read. Recover, but only after the
            // finally clears readInProgress (recoverConnection -> connectOnce reads the socket).
            $closed = true;
            $chunk = '';
        } catch (\Throwable) {
            // No PONG within the window (or a transient read error); leave escalation to the next
            // tick or to the application's own processIncoming() loop.
            return;
        } finally {
            $this->readInProgress = false;
        }

        if ($closed) {
            try {
                $this->recoverConnection();
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
        }

        if ($chunk === '') {
            return;
        }

        try {
            foreach ($this->parser->push($chunk) as $frame) {
                $this->handleFrame($frame);
            }

            // The PONG handled above (handleFrame) resets the outstanding-ping counter; do not reset
            // on any other frame, so an unresponsive server still trips maxPingsOut.

            // Deliver any message frames captured during the heartbeat read instead of leaving
            // them buffered until the next processIncoming(), mirroring processIncoming().
            $this->drainAllPending();
        } catch (\Throwable) {
            // A fatal frame surfaced during the heartbeat read; let the next user read / tick
            // surface and act on it rather than throwing out of the event-loop timer.
        }
    }

    /**
     * Cancels the active ping timer if running.
     */
    private function cancelPingTimer(): void
    {
        if ($this->pingTimerId !== null) {
            EventLoop::cancel($this->pingTimerId);
            $this->pingTimerId = null;
        }
    }

    /**
     * Validates a NATS subject string against protocol rules.
     *
     * @param bool $allowWildcards Whether * and > tokens are permitted (subscribe only).
     */
    private function validateSubject(string $subject, bool $allowWildcards = false): void
    {
        if ($subject === '') {
            throw new ProtocolException('Subject must not be empty');
        }

        if (preg_match('/[\s\r\n]/', $subject)) {
            throw new ProtocolException('Subject must not contain whitespace');
        }

        $tokens = explode('.', $subject);
        foreach ($tokens as $i => $token) {
            if ($token === '') {
                throw new ProtocolException('Subject must not contain empty tokens');
            }

            if ($token === '*' || $token === '>') {
                if (!$allowWildcards) {
                    throw new ProtocolException('Wildcards are not allowed in publish subjects');
                }

                // ">" must be the last token.
                if ($token === '>' && $i !== count($tokens) - 1) {
                    throw new ProtocolException('Wildcard ">" must be the last token');
                }

                continue;
            }

            if (str_contains($token, '*') || str_contains($token, '>')) {
                throw new ProtocolException('Wildcards must occupy an entire token');
            }
        }
    }

    /**
     * Validates a NATS queue group name against protocol rules.
     *
     * Queue groups are interpolated into the SUB control line, so they must not
     * be empty or contain whitespace/CR/LF that could break or inject wire frames.
     */
    private function validateQueueGroup(string $queue): void
    {
        if ($queue === '') {
            throw new ProtocolException('Queue group must not be empty');
        }

        if (preg_match('/[\s\r\n]/', $queue)) {
            throw new ProtocolException('Queue group must not contain whitespace');
        }
    }

    /**
     * Delivers buffered messages to a single subscription callback in FIFO order.
     */
    private function drainPendingForSid(int $sid): void
    {
        if (!isset($this->pendingMessages[$sid])) {
            return;
        }

        if (!isset($this->subscriptions[$sid])) {
            // The subscription is gone; its backlog is undeliverable. Drop it instead of retaining an
            // entry that drainAllPending() would re-scan on every chunk.
            unset($this->pendingMessages[$sid]);

            return;
        }

        if (isset($this->dispatchingSids[$sid])) {
            // Already delivering this sid further up the stack (a handler awaited and suspended). Do
            // not re-enter: the suspended loop resumes and drains whatever we enqueued meanwhile, so
            // ordering holds and a handler is never invoked on top of itself.
            return;
        }

        $this->dispatchingSids[$sid] = true;

        try {
            $queue = $this->pendingMessages[$sid];

            while (!$queue->isEmpty()) {
                if (!array_key_exists($sid, $this->subscriptions)) {
                    break;
                }

                /** @var NatsMessage $message */
                $message = $queue->dequeue();
                $this->subscriptions[$sid]($message);
            }

            if ($queue->isEmpty()) {
                // Don't retain a drained (empty) queue: keep drainAllPending()'s per-chunk scan
                // proportional to the subscriptions that actually have pending messages, not every
                // subscription that has ever received one.
                unset($this->pendingMessages[$sid]);
            }
        } finally {
            unset($this->dispatchingSids[$sid]);
        }
    }

    private function cleanupRequestSubscription(int $sid): void
    {
        // Clean up based on the subscription itself, not on a pending message queue: the queue is
        // created lazily and removed once drained, so requiring it here would skip the UNSUB for any
        // request that actually received a reply.
        if (!isset($this->subscriptionMeta[$sid], $this->subscriptions[$sid])) {
            return;
        }

        if ($this->state === ConnectionState::Open) {
            try {
                $this->unsubscribe($sid)->await();

                return;
            } catch (\Throwable) {
                // Preserve the original request failure and fall back to local cleanup.
            }
        }

        $this->dropSubscriptionState($sid);
    }

    private function dropSubscriptionState(int $sid): void
    {
        unset($this->subscriptions[$sid]);
        unset($this->subscriptionMeta[$sid]);
        unset($this->pendingMessages[$sid]);
    }
}
