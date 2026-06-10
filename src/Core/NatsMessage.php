<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\Future;

/**
 * Immutable inbound NATS message representation.
 */
final class NatsMessage
{
    /**
     * Represents a normalized delivery passed to user subscription handlers.
     *
     * @param string $subject Subject that matched the subscription and carried this message.
     * @param int $sid Subscription ID assigned by this client for the matching SUB command.
     * @param string|null $replyTo Optional reply subject set by publisher for request/reply flows.
     * @param string $payload Message payload bytes as decoded by protocol parser.
     * @param string|null $rawHeaders Raw NATS/1.0 header block for HMSG frames, including trailing CRLF section.
     * @param (\Closure(string,string,array<string,string>|null):Future<void>)|null $responder Publish callback
     *        bound by the delivering connection so the message can reply to its own {@see $replyTo}. Null for
     *        messages that were not delivered through a live connection (they cannot {@see respond()}).
     */
    public function __construct(
        public readonly string $subject,
        public readonly int $sid,
        public readonly ?string $replyTo,
        public readonly string $payload,
        public readonly ?string $rawHeaders = null,
        private readonly ?\Closure $responder = null,
    ) {}

    /**
     * Replies to this message on its {@see $replyTo} subject (request/reply or a JetStream-style
     * service handler). Equivalent to nats.go's `Msg.Respond()` / nats.java's `Message.respond()`.
     *
     * @return Future<void>
     *
     * @throws \LogicException When the message has no reply subject, or was not delivered through a
     *                         live connection (no responder bound).
     */
    public function respond(string $payload): Future
    {
        return $this->respondWith($payload, null);
    }

    /**
     * Replies to this message on its {@see $replyTo} subject with NATS headers.
     *
     * @param array<string,string> $headers
     * @return Future<void>
     *
     * @throws \LogicException When the message has no reply subject, or was not delivered through a
     *                         live connection (no responder bound).
     */
    public function respondWithHeaders(string $payload, array $headers): Future
    {
        return $this->respondWith($payload, $headers);
    }

    /**
     * Whether this message can be replied to ({@see respond()} will not throw): it carries a reply
     * subject and is bound to a live connection.
     */
    public function isReplyable(): bool
    {
        return $this->replyTo !== null && $this->replyTo !== '' && $this->responder !== null;
    }

    /**
     * @param array<string,string>|null $headers
     * @return Future<void>
     */
    private function respondWith(string $payload, ?array $headers): Future
    {
        if ($this->replyTo === null || $this->replyTo === '') {
            throw new \LogicException('Cannot respond to a message that has no reply subject');
        }

        if ($this->responder === null) {
            throw new \LogicException('Cannot respond: this message is not bound to a live connection');
        }

        return ($this->responder)($this->replyTo, $payload, $headers);
    }
}
