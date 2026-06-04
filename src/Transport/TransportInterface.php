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
     * Implementations that support TLS must honor the `tlsHandshakeFirst` option:
     * - When true, perform the TLS handshake immediately after the TCP connect.
     * - When false (default), establish the plain TCP connection only; the caller
     *   will request a TLS upgrade via {@see setupTls()} once the server INFO
     *   frame advertises `tls_required`.
     *
     * @return Future<void>
     */
    public function connect(string $dsn, int $timeoutMs): Future;

    /**
     * Performs the TLS upgrade on an already-connected socket (upgrade-after-INFO mode).
     *
     * Implementations must be a no-op when TLS has already been negotiated
     * (handshake-first mode) or when no TLS context is configured.
     *
     * @return Future<void>
     */
    public function setupTls(int $timeoutMs): Future;

    /**
     * Writes raw protocol bytes to the transport.
     *
     * @return Future<void>
     */
    public function write(string $bytes): Future;

    /**
     * Reads a raw chunk from the transport.
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
