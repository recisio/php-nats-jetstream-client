<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Future;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;

use function Amp\async;

/**
 * Fluent builder for pull-consumer batch iteration.
 *
 * Usage:
 *   $js->pullConsumer('STREAM', 'CONSUMER')
 *      ->setBatching(10)
 *      ->setExpiresMs(5000)
 *      ->setIterations(3)
 *      ->handle(function (NatsMessage $msg, JetStreamContext $js) {
 *          $js->ack($msg)->await();
 *      });
 */
final class PullConsumerIterator
{
    private int $batch = 1;
    private int $expiresMs = 3000;
    private ?int $iterations = null;

    /**
     * @param JetStreamContext $context JetStream context used to issue pull requests and ACK-related commands.
     * @param string $stream Stream name that owns the target consumer.
     * @param string $consumer Durable/ephemeral consumer name used for `CONSUMER.MSG.NEXT` pulls.
     */
    public function __construct(
        private readonly JetStreamContext $context,
        private readonly string $stream,
        private readonly string $consumer,
    ) {}

    /**
     * Sets the number of messages to fetch per pull request.
     *
     * @return $this
     */
    public function setBatching(int $batch): self
    {
        if ($batch <= 0) {
            throw new JetStreamException('Batch size must be greater than zero');
        }
        $this->batch = $batch;

        return $this;
    }

    /**
     * Sets the server-side expiration timeout in milliseconds for each pull request.
     *
     * @return $this
     */
    public function setExpiresMs(int $expiresMs): self
    {
        if ($expiresMs <= 0) {
            throw new JetStreamException('ExpiresMs must be greater than zero');
        }
        $this->expiresMs = $expiresMs;

        return $this;
    }

    /**
     * Sets the number of fetch iterations (null = infinite loop).
     *
     * @return $this
     */
    public function setIterations(?int $iterations): self
    {
        if ($iterations !== null && $iterations <= 0) {
            throw new JetStreamException('Iterations must be greater than zero or null for infinite');
        }
        $this->iterations = $iterations;

        return $this;
    }

    /**
     * Returns configured batch size.
     */
    public function getBatching(): int
    {
        return $this->batch;
    }

    /**
     * Returns configured server-side expiration.
     */
    public function getExpiresMs(): int
    {
        return $this->expiresMs;
    }

    /**
     * Returns configured iterations (null = infinite).
     */
    public function getIterations(): ?int
    {
        return $this->iterations;
    }

    /**
     * Runs the fetch loop, invoking the handler for each received message.
     *
     * @param callable(NatsMessage, JetStreamContext):void $handler
     * @return Future<int> Total number of messages processed.
     */
    public function handle(callable $handler): Future
    {
        return async(function () use ($handler): int {
            $totalProcessed = 0;
            $iteration = 0;

            while ($this->iterations === null || $iteration < $this->iterations) {
                ++$iteration;

                try {
                    $messages = $this->context->fetchBatch(
                        $this->stream,
                        $this->consumer,
                        $this->batch,
                        $this->expiresMs,
                    )->await();
                } catch (JetStreamException $e) {
                    // In infinite mode keep polling through routine/transient conditions so a
                    // long-running worker is not killed by a quiet period, backpressure, or a
                    // failover: 404 (no messages) and 408 (request timeout) are routine empty
                    // windows, and a 409 may be transient (MaxAckPending exceeded / leadership
                    // change / server shutdown / max-waiting) rather than terminal (Consumer
                    // Deleted). Finite mode keeps the existing stop-on-any-error behavior.
                    if ($this->iterations === null) {
                        $code = $e->getCode();
                        if (in_array($code, [404, 408], true) || ($code === 409 && self::isTransientPullStatus($e->getMessage()))) {
                            continue;
                        }
                    }

                    // Finite mode, or a terminal error (e.g. 409 Consumer Deleted): stop iterating.
                    break;
                }

                foreach ($messages as $message) {
                    $handler($message, $this->context);
                    ++$totalProcessed;
                }
            }

            return $totalProcessed;
        });
    }

    /**
     * Whether a 409 pull status describes a transient, self-clearing condition that an infinite
     * worker should keep polling through (as opposed to a terminal one such as "Consumer Deleted").
     */
    private static function isTransientPullStatus(string $message): bool
    {
        foreach (['MaxAckPending', 'Leadership Change', 'Server Shutdown', 'Exceeded MaxWaiting'] as $needle) {
            if (stripos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
    }
}
