<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use PHPUnit\Framework\TestCase;

/**
 * Empirically verifies cross-client interoperability with the official `nats` CLI (nats.go under the
 * hood): KeyValue and Object Store buckets written by this client are readable by the CLI and vice
 * versa. The Object Store meta check is also the live regression for #109 (empty metadata previously
 * serialized as a JSON array, which the official client rejects as "object-store meta information
 * invalid").
 *
 * The `nats` command is resolved from `$NATS_CLI`, then a host `nats` binary, then a `natsio/nats-box`
 * Docker image (run with host networking). The whole class is skipped when none is available.
 */
final class NatsCliInteropIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /** @var list<string>|null */
    private ?array $cliPrefix = null;

    protected function setUp(): void
    {
        $this->requireIntegrationEnabled();

        $prefix = $this->resolveNatsCliPrefix();
        if ($prefix === null) {
            self::markTestSkipped('No `nats` CLI available (set $NATS_CLI, install `nats`, or provide Docker for nats-box).');
        }

        $this->cliPrefix = $prefix;
    }

    public function testKeyValueWrittenByThisClientIsReadableByNatsCli(): void
    {
        $bucket = 'iopkv' . bin2hex(random_bytes(3));
        $client = $this->connect();

        try {
            $kv = $client->jetStream()->keyValue($bucket);
            $kv->create()->await();
            $kv->put('greeting', 'hello-from-php')->await();

            $result = $this->runNats(['kv', 'get', $bucket, 'greeting', '--raw']);
            self::assertSame(0, $result['code'], 'nats kv get failed: ' . $result['err']);
            self::assertSame('hello-from-php', trim($result['out']));
        } finally {
            $this->runNats(['kv', 'del', '--force', $bucket]);
            $client->disconnect()->await();
        }
    }

    public function testKeyValueWrittenByNatsCliIsReadableByThisClient(): void
    {
        $bucket = 'ioqkv' . bin2hex(random_bytes(3));

        $add = $this->runNats(['kv', 'add', $bucket, '--history=5']);
        self::assertSame(0, $add['code'], 'nats kv add failed: ' . $add['err']);

        $client = $this->connect();

        try {
            $put = $this->runNats(['kv', 'put', $bucket, 'fromcli', 'hello-from-cli']);
            self::assertSame(0, $put['code'], 'nats kv put failed: ' . $put['err']);

            $entry = $client->jetStream()->keyValue($bucket)->get('fromcli')->await();
            self::assertNotNull($entry);
            self::assertSame('hello-from-cli', $entry->value);
        } finally {
            $this->runNats(['kv', 'del', '--force', $bucket]);
            $client->disconnect()->await();
        }
    }

    public function testObjectStoreMetaWrittenByThisClientIsReadableByNatsCli(): void
    {
        // #109: an object stored with default (empty) metadata must be readable by the official client.
        // Before the fix `nats object info` returned "object-store meta information invalid".
        $bucket = 'iopobj' . bin2hex(random_bytes(3));
        $client = $this->connect();

        try {
            $store = $client->jetStream()->objectStore($bucket);
            $store->create()->await();
            $store->put('doc.txt', 'object-from-php')->await(); // no metadata -> the #109 case

            $info = $this->runNats(['object', 'info', $bucket, 'doc.txt']);
            self::assertSame(0, $info['code'], 'nats object info failed (meta not interoperable): ' . $info['err']);
            self::assertStringContainsString('doc.txt', $info['out']);
            self::assertStringNotContainsStringIgnoringCase('invalid', $info['err']);
        } finally {
            $this->runNats(['object', 'rm', '--force', $bucket]);
            $client->disconnect()->await();
        }
    }

    public function testObjectStoreWrittenByNatsCliIsReadableByThisClient(): void
    {
        $bucket = 'ioqobj' . bin2hex(random_bytes(3));

        $add = $this->runNats(['object', 'add', $bucket]);
        self::assertSame(0, $add['code'], 'nats object add failed: ' . $add['err']);

        $put = $this->runNats(['object', 'put', $bucket, '--name', 'doc.txt'], 'object-from-cli');
        self::assertSame(0, $put['code'], 'nats object put failed: ' . $put['err']);

        $client = $this->connect();

        try {
            $object = $client->jetStream()->objectStore($bucket)->get('doc.txt')->await();
            self::assertNotNull($object);
            self::assertSame('object-from-cli', $object->data);
        } finally {
            $this->runNats(['object', 'rm', '--force', $bucket]);
            $client->disconnect()->await();
        }
    }

    private function connect(): NatsClient
    {
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        return $client;
    }

    /**
     * @param list<string> $args
     * @return array{code: int, out: string, err: string}
     */
    private function runNats(array $args, ?string $stdin = null): array
    {
        $command = array_merge($this->cliPrefix ?? [], ['-s', $this->integrationServerUrl()], $args);

        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $pipes = [];
        $process = proc_open($command, $descriptors, $pipes);
        if (!is_resource($process)) {
            self::fail('Failed to launch nats CLI: ' . implode(' ', $command));
        }

        if ($stdin !== null) {
            fwrite($pipes[0], $stdin);
        }
        fclose($pipes[0]);

        $out = stream_get_contents($pipes[1]);
        $err = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        $code = proc_close($process);

        return [
            'code' => $code,
            'out' => $out === false ? '' : $out,
            'err' => $err === false ? '' : $err,
        ];
    }

    /**
     * @return list<string>|null
     */
    private function resolveNatsCliPrefix(): ?array
    {
        $configured = getenv('NATS_CLI');
        if (is_string($configured) && trim($configured) !== '') {
            $parts = preg_split('/\s+/', trim($configured));

            return $parts === false ? null : $parts;
        }

        if ($this->commandExists('nats')) {
            return ['nats'];
        }

        if ($this->commandExists('docker')) {
            return ['docker', 'run', '--rm', '-i', '--network', 'host', 'natsio/nats-box:latest', 'nats'];
        }

        return null;
    }

    private function commandExists(string $binary): bool
    {
        $output = [];
        $code = 0;
        exec('command -v ' . escapeshellarg($binary) . ' 2>/dev/null', $output, $code);

        return $code === 0;
    }
}
