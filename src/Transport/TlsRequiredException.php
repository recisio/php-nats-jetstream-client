<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use IDCT\NATS\Exception\NatsThrowable;

/**
 * Thrown when a TLS upgrade is requested (the server or options require TLS) but no TLS context was
 * configured at connect time, so the handshake cannot run.
 *
 * Failing here prevents the connection from writing CONNECT (which carries credentials) over a
 * still-plaintext socket. Extends \RuntimeException so connect()'s catch (\Throwable) wraps it into
 * a ConnectionException, and implements {@see NatsThrowable} so it is catchable as a library error.
 */
final class TlsRequiredException extends \RuntimeException implements NatsThrowable {}
