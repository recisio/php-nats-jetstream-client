<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

/**
 * Immutable snapshot of connection traffic counters, mirroring nats.go `Statistics` /
 * nats.java `Statistics`.
 */
final class ConnectionStats
{
    /**
     * @param int $inMsgs    Messages delivered to subscriptions on this connection.
     * @param int $outMsgs   Messages published from this connection.
     * @param int $inBytes   Payload bytes received (message bodies; excludes protocol framing).
     * @param int $outBytes  Payload bytes published (message bodies; excludes protocol framing).
     * @param int $reconnects Number of successful reconnects performed.
     */
    public function __construct(
        public readonly int $inMsgs,
        public readonly int $outMsgs,
        public readonly int $inBytes,
        public readonly int $outBytes,
        public readonly int $reconnects,
    ) {}
}
