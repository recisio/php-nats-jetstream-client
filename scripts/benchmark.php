<?php

declare(strict_types=1);

/**
 * Local performance baseline: request/reply round-trip rate and fire-and-forget publish throughput,
 * single process, against a running NATS server.
 *
 * Usage:
 *   NATS_URL=nats://127.0.0.1:4222 BENCH_ITER=5000 php scripts/benchmark.php
 *
 * Numbers are environment-specific (CPU, loopback vs network, server config) and meant as a relative
 * baseline, not an absolute guarantee.
 */

require __DIR__ . '/../vendor/autoload.php';

use Amp\CancelledException;
use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

use function Amp\async;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';
$iterations = (int) (getenv('BENCH_ITER') ?: '5000');
$payload = str_repeat('x', (int) (getenv('BENCH_PAYLOAD') ?: '16'));

$server = new NatsClient(new NatsOptions(servers: [$url]));
$client = new NatsClient(new NatsOptions(servers: [$url]));
$server->connect()->await();
$client->connect()->await();

echo 'server=' . $url . ' iterations=' . $iterations . ' payload_bytes=' . strlen($payload) . PHP_EOL;

// ── Request/reply round-trip rate ──────────────────────────────────────────────────────────────────
$server->subscribe('bench.echo', static function (NatsMessage $message) use ($server): void {
    if ($message->replyTo !== null) {
        $server->publish($message->replyTo, 'ok')->await();
    }
})->await();

$serverCancel = new DeferredCancellation();
$serverLoop = async(static function () use ($server, $serverCancel): void {
    $cancellation = $serverCancel->getCancellation();
    while (!$cancellation->isRequested()) {
        try {
            $server->processIncoming($cancellation)->await();
        } catch (CancelledException) {
            break;
        }
    }
});

$start = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $client->request('bench.echo', $payload, 2000)->await();
}
$rrNs = hrtime(true) - $start;

$serverCancel->cancel();
$serverLoop->await();

$rrRps = $iterations / max(1e-9, $rrNs / 1_000_000_000);
echo sprintf('request_reply: %d in %.1f ms => %s req/s (%.3f ms/req)', $iterations, $rrNs / 1e6, number_format($rrRps, 0), ($rrNs / 1e6) / $iterations) . PHP_EOL;

// ── Fire-and-forget publish throughput ─────────────────────────────────────────────────────────────
$start = hrtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $client->publish('bench.pub', $payload)->await();
}
// Round-trip a PING/PONG to flush everything through the socket before stopping the clock.
$client->flush()->await();
$pubNs = hrtime(true) - $start;

$pubRps = $iterations / max(1e-9, $pubNs / 1_000_000_000);
echo sprintf('publish: %d in %.1f ms => %s msg/s', $iterations, $pubNs / 1e6, number_format($pubRps, 0)) . PHP_EOL;

$client->disconnect()->await();
$server->disconnect()->await();
