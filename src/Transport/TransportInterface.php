<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;

/**
 * Abstraction for asynchronous socket operations used by the NATS connection.
 */
interface TransportInterface
{
    /**
     * Establishes a transport connection to the target DSN.
     *
     * @return Future<void>
     */
    public function connect(string $dsn, int $timeoutMs): Future;

    /**
     * Performs a TLS handshake on the already-connected socket (standard post-INFO upgrade).
     *
     * Implementations that already negotiated TLS during {@see connect()} (handshake-first) must
     * treat this as a no-op so callers can invoke it unconditionally.
     *
     * @return Future<void>
     */
    public function upgradeTls(): Future;

    /**
     * Writes raw protocol bytes to the transport.
     *
     * @return Future<void>
     */
    public function write(string $bytes): Future;

    /**
     * Reads a raw chunk from the transport.
     *
     * Implementations MUST signal a peer close (EOF) on a live socket by throwing
     * {@see TransportClosedException}, so the connection layer can reconnect from the read path. An
     * empty string MUST be reserved for "no bytes available without EOF" (e.g. no socket yet). A
     * supplied non-null cancellation MUST be honored (a read timeout surfaces as an Amp
     * CancelledException, never as EOF); with a null cancellation the read may suspend until data
     * arrives or the peer closes.
     *
     * @return Future<string>
     */
    public function readLine(?Cancellation $cancellation = null): Future;

    /**
     * Closes the transport and underlying resources.
     *
     * @return Future<void>
     */
    public function close(): Future;
}
