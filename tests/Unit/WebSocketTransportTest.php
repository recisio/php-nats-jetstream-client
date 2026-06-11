<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

final class WebSocketTransportTest extends TestCase
{
    /**
     * Verifies the WebSocket transport is TLS-aware and reports no TLS before connecting (#31).
     */
    public function testIsTlsAwareAndInactiveBeforeConnect(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        self::assertInstanceOf(TlsAwareTransportInterface::class, $transport);
        self::assertFalse($transport->tlsActive());
    }

    /**
     * Verifies readLine() returns '' (not EOF) when no socket is connected yet (#31).
     */
    public function testReadLineReturnsEmptyWithoutSocket(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        self::assertSame('', $transport->readLine()->await());
    }

    /**
     * Verifies upgradeTls() is a no-op for WebSocket (TLS is done at connect) (#31).
     */
    public function testUpgradeTlsIsNoOp(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        // Resolves without error and leaves TLS inactive (wss negotiates during connect()).
        $transport->upgradeTls()->await();
        self::assertFalse($transport->tlsActive());
    }

    /**
     * Verifies the upgrade request includes custom headers and the compression offer (#61).
     */
    public function testBuildUpgradeRequestWithCustomHeadersAndCompression(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest(
            'nats.example',
            443,
            '/',
            'abc123==',
            ['Cookie' => 'session=xyz', 'X-Proxy-Auth' => 'token'],
            true,
        );

        self::assertStringContainsString("GET / HTTP/1.1\r\n", $request);
        self::assertStringContainsString("Host: nats.example:443\r\n", $request);
        self::assertStringContainsString("Sec-WebSocket-Key: abc123==\r\n", $request);
        self::assertStringContainsString('Sec-WebSocket-Extensions: permessage-deflate', $request);
        self::assertStringContainsString("Cookie: session=xyz\r\n", $request);
        self::assertStringContainsString("X-Proxy-Auth: token\r\n", $request);
        self::assertStringEndsWith("\r\n\r\n", $request);
    }

    /**
     * Verifies reserved headers cannot be overridden and CR/LF is stripped from custom values (#61).
     */
    public function testBuildUpgradeRequestRejectsReservedAndStripsCrLf(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest(
            'h',
            80,
            '/',
            'k',
            ['Host' => 'evil', 'X-Inject' => "ok\r\nX-Evil: 1"],
            false,
        );

        // The reserved Host header keeps its real value (the override is ignored).
        self::assertStringContainsString("Host: h:80\r\n", $request);
        self::assertStringNotContainsString('evil', $request);
        // CR/LF stripped from a custom value: no injected header line.
        self::assertStringContainsString("X-Inject: okX-Evil: 1\r\n", $request);
        self::assertStringNotContainsString("\r\nX-Evil: 1\r\n", $request);
        // No compression offer when disabled.
        self::assertStringNotContainsString('permessage-deflate', $request);
    }

    /**
     * Verifies connect() rejects a DSN without a host before attempting a socket connection (#31).
     */
    public function testConnectRejectsDsnWithoutHost(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Invalid WebSocket DSN');
        $transport->connect('ws:///just-a-path', 1000)->await();
    }
}
