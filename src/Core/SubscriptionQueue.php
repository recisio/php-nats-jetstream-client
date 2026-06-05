<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use SplQueue;

use function Amp\delay;

/**
 * A queue-based subscription that collects messages for polling access.
 *
 * Provides `fetch()`, `next()`, and `fetchAll()` methods similar to
 * basis-company/nats.php queue interface.
 */
final class SubscriptionQueue
{
    /** @var SplQueue<NatsMessage> */
    private SplQueue $messages;
    private float $timeout = 0;

    /**
     * @param NatsClient $client Client used to drive processIncoming polling while waiting for queued deliveries.
     * @param int $sid Subscription ID backing this queue instance.
     */
    public function __construct(
        private readonly NatsClient $client,
        public readonly int $sid,
    ) {
        $this->messages = new SplQueue();
    }

    /**
     * Enqueues a message received from the subscription callback.
     *
     * @internal Called by the subscribe handler.
     */
    public function enqueue(NatsMessage $message): void
    {
        $this->messages->enqueue($message);
    }

    /**
     * Sets the timeout in seconds for blocking operations.
     *
     * @return $this
     */
    public function setTimeout(float $seconds): self
    {
        $this->timeout = $seconds;

        return $this;
    }

    /**
     * Returns the next buffered message without blocking, or null if none available.
     */
    public function fetch(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        // Try one processIncoming cycle to see if anything arrives.
        $this->client->processIncoming()->await();

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Blocks until a message is available, up to the configured timeout.
     *
     * Returns the next message, or `null` when no message arrives within the configured timeout.
     * When no timeout is configured (`timeout <= 0`) a single `processIncoming()` cycle is
     * attempted, matching {@see fetch()} rather than blocking indefinitely.
     */
    public function next(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        if ($this->timeout <= 0) {
            $this->client->processIncoming()->await();

            return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
        }

        // Bound the wait with a cancellation so the underlying socket read cannot block past the
        // configured timeout, then yield cooperatively between cycles to avoid a tight spin.
        $cancellation = new TimeoutCancellation($this->timeout);
        $deadline = microtime(true) + $this->timeout;

        try {
            do {
                $this->client->processIncoming($cancellation)->await();

                if ($this->messages->count() > 0) {
                    break;
                }

                delay(0.001, cancellation: $cancellation);
            } while (microtime(true) < $deadline);
        } catch (CancelledException) {
            // Timeout elapsed without a message.
        }

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Fetches all available messages up to the given limit.
     *
     * Uses the configured timeout to wait for messages. Returns early
     * if no new messages arrive after a processIncoming cycle.
     *
     * @param int|null $limit Maximum messages to return (null = unlimited).
     * @return list<NatsMessage>
     */
    public function fetchAll(?int $limit = null): array
    {
        $collected = [];

        // Drain anything already buffered first.
        while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
            $collected[] = $this->messages->dequeue();
        }

        if ($limit !== null && count($collected) >= $limit) {
            return $collected;
        }

        // A cancellation bounds each read so a real socket cannot block past the timeout window.
        $cancellation = $this->timeout > 0 ? new TimeoutCancellation($this->timeout) : null;

        try {
            while ($limit === null || count($collected) < $limit) {
                $frames = $this->client->processIncoming($cancellation)->await();

                while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
                    $collected[] = $this->messages->dequeue();
                }

                if ($frames === 0 && $this->messages->isEmpty()) {
                    break;
                }
            }
        } catch (CancelledException) {
            // Timeout window elapsed; return what was collected.
        }

        // Final drain of any remaining buffered messages.
        while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
            $collected[] = $this->messages->dequeue();
        }

        return $collected;
    }
}
