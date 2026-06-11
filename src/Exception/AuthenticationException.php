<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Raised when the server rejects the connection for an authentication/authorization reason (bad
 * credentials, authorization violation, expired/invalid JWT). Because such a failure will not resolve
 * by retrying, the reconnect loop terminates immediately rather than exhausting its attempts.
 */
class AuthenticationException extends ConnectionException {}
