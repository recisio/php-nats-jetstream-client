<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

/**
 * Thrown when a TLS upgrade is requested (the server or options require TLS) but no TLS context was
 * configured at connect time, so the handshake cannot run.
 *
 * Failing here prevents the connection from writing CONNECT (which carries credentials) over a
 * still-plaintext socket. Extends \RuntimeException so connect()'s catch (\Throwable) wraps it into
 * a ConnectionException.
 */
final class TlsRequiredException extends \RuntimeException {}
