<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when connection establishment or transport state becomes invalid.
 *
 * Not final: {@see AuthenticationException} specializes it for auth/authorization failures so callers
 * can catch those distinctly while existing `catch (ConnectionException)` handlers still match.
 */
class ConnectionException extends NatsException {}
