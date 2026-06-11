<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;

use function Amp\async;
use function Amp\Socket\connect;

/**
 * NATS-over-WebSocket transport (`ws://` / `wss://`).
 *
 * After a TCP (and, for `wss://`, TLS) connect it performs the RFC 6455 HTTP upgrade handshake, then
 * carries raw NATS protocol bytes as WebSocket binary frames: outbound writes are masked client
 * frames; inbound reads are decoded (and reassembled) via {@see WebSocketFrameCodec}, with ping/close
 * control frames handled transparently. TLS for `wss://` is negotiated during {@see connect()}, so
 * {@see upgradeTls()} is a no-op (there is no separate post-INFO upgrade for WebSocket).
 */
final class WebSocketTransport implements TlsAwareTransportInterface
{
    private ?Socket $socket = null;
    private int $lastConnectTimeoutMs = 5_000;
    private bool $tlsEstablished = false;

    /** Raw (post-handshake) bytes received but not yet decoded into complete frames. */
    private string $readBuffer = '';

    /** Reassembly buffer for a fragmented data message and whether one is in progress. */
    private string $fragmentBuffer = '';
    private bool $fragmenting = false;

    /**
     * @param NatsOptions $options Client options controlling TLS (used for `wss://`) and socket behavior.
     */
    public function __construct(private readonly NatsOptions $options = new NatsOptions()) {}

    /**
     * Connects to a `ws://` or `wss://` NATS endpoint and completes the WebSocket upgrade handshake.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->lastConnectTimeoutMs = max(1, $timeoutMs);
            $this->tlsEstablished = false;
            $this->readBuffer = '';
            $this->fragmentBuffer = '';
            $this->fragmenting = false;

            $parts = parse_url($dsn);
            if ($parts === false || !isset($parts['host'])) {
                throw new ConnectionException('Invalid WebSocket DSN: ' . $dsn);
            }

            $scheme = strtolower($parts['scheme'] ?? 'ws');
            $secure = $scheme === 'wss';
            $host = $parts['host'];
            $port = $parts['port'] ?? ($secure ? 443 : 80);
            $path = ($parts['path'] ?? '') === '' ? '/' : $parts['path'];
            if (isset($parts['query'])) {
                $path .= '?' . $parts['query'];
            }

            $context = (new ConnectContext())
                ->withConnectTimeout($this->lastConnectTimeoutMs / 1000)
                ->withTcpNoDelay();
            if ($secure) {
                $context = $context->withTlsContext($this->buildTlsContext($host));
            }

            $this->socket = connect("tcp://{$host}:{$port}", $context);

            if ($secure) {
                $this->socket->setupTls(new TimeoutCancellation($this->lastConnectTimeoutMs / 1000));
                $this->tlsEstablished = true;
            }

            $this->performHandshake($host, $port, $path);
        });
    }

    /**
     * No-op: for WebSocket, TLS (`wss://`) is established during {@see connect()}, not as a separate
     * post-INFO upgrade.
     */
    public function upgradeTls(): Future
    {
        return async(static function (): void {});
    }

    /**
     * Reports whether the `wss://` TLS handshake has completed.
     */
    public function tlsActive(): bool
    {
        return $this->tlsEstablished;
    }

    /**
     * Writes NATS protocol bytes as a masked WebSocket binary frame.
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $this->socket?->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $bytes));
        });
    }

    /**
     * Reads the next available decoded NATS bytes, transparently answering pings and reassembling
     * fragmented messages. Throws {@see TransportClosedException} on a close frame or peer EOF.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            if ($this->socket === null) {
                return '';
            }

            while (true) {
                $data = $this->drainDataFrames();
                if ($data !== '') {
                    return $data;
                }

                $chunk = $this->socket->read($cancellation);
                if ($chunk === null) {
                    throw new TransportClosedException('WebSocket closed by peer (EOF)');
                }

                $this->readBuffer .= $chunk;
            }
        });
    }

    /**
     * Sends a close frame (best effort) and closes the underlying socket.
     */
    public function close(): Future
    {
        return async(function (): void {
            $socket = $this->socket;
            $this->socket = null;
            if ($socket === null) {
                return;
            }

            try {
                $socket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, ''));
            } catch (\Throwable) {
                // The socket may already be gone; closing below is what matters.
            }

            $socket->close();
        });
    }

    /**
     * Decodes whatever complete frames are buffered, handling control frames inline and returning the
     * concatenated payload of any completed data messages ('' when none are ready yet).
     */
    private function drainDataFrames(): string
    {
        $frames = WebSocketFrameCodec::decode($this->readBuffer);
        $out = '';

        foreach ($frames as $frame) {
            switch ($frame['opcode']) {
                case WebSocketFrameCodec::OP_PING:
                    // Answer with a pong carrying the same application data.
                    $this->socket?->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PONG, $frame['payload']));
                    break;

                case WebSocketFrameCodec::OP_PONG:
                    break;

                case WebSocketFrameCodec::OP_CLOSE:
                    throw new TransportClosedException('WebSocket close frame received');

                case WebSocketFrameCodec::OP_BINARY:
                case WebSocketFrameCodec::OP_TEXT:
                    if ($frame['fin']) {
                        $out .= $frame['payload'];
                    } else {
                        $this->fragmentBuffer = $frame['payload'];
                        $this->fragmenting = true;
                    }
                    break;

                case WebSocketFrameCodec::OP_CONTINUATION:
                    if ($this->fragmenting) {
                        $this->fragmentBuffer .= $frame['payload'];
                        if ($frame['fin']) {
                            $out .= $this->fragmentBuffer;
                            $this->fragmentBuffer = '';
                            $this->fragmenting = false;
                        }
                    }
                    break;
            }
        }

        return $out;
    }

    /**
     * Sends the HTTP upgrade request and validates the server's 101 response (status + accept key).
     * Any bytes the server sent after the header terminator (e.g. the NATS INFO frame) are retained.
     */
    private function performHandshake(string $host, int $port, string $path): void
    {
        $socket = $this->socket;
        if ($socket === null) {
            throw new ConnectionException('WebSocket socket not connected');
        }

        $clientKey = WebSocketFrameCodec::generateClientKey();
        $request = "GET {$path} HTTP/1.1\r\n"
            . "Host: {$host}:{$port}\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Key: {$clientKey}\r\n"
            . "Sec-WebSocket-Version: 13\r\n\r\n";
        $socket->write($request);

        $cancellation = new TimeoutCancellation($this->lastConnectTimeoutMs / 1000);
        $response = '';
        while (!str_contains($response, "\r\n\r\n")) {
            $chunk = $socket->read($cancellation);
            if ($chunk === null) {
                throw new ConnectionException('WebSocket handshake failed: connection closed before response');
            }
            $response .= $chunk;
            if (strlen($response) > 16384) {
                throw new ConnectionException('WebSocket handshake response exceeded the maximum header size');
            }
        }

        $separator = (int) strpos($response, "\r\n\r\n");
        $header = substr($response, 0, $separator);
        // Surplus bytes after the header belong to the WebSocket stream (e.g. the NATS INFO frame).
        $this->readBuffer = substr($response, $separator + 4);

        $lines = explode("\r\n", $header);
        $statusLine = $lines[0];
        if (preg_match('#^HTTP/1\.[01]\s+101\b#', $statusLine) !== 1) {
            throw new ConnectionException('WebSocket upgrade rejected by server: ' . $statusLine);
        }

        $accept = null;
        foreach (array_slice($lines, 1) as $line) {
            $colon = strpos($line, ':');
            if ($colon === false) {
                continue;
            }
            if (strcasecmp(trim(substr($line, 0, $colon)), 'Sec-WebSocket-Accept') === 0) {
                $accept = trim(substr($line, $colon + 1));
                break;
            }
        }

        if ($accept === null || !hash_equals(WebSocketFrameCodec::acceptKey($clientKey), $accept)) {
            throw new ConnectionException('WebSocket handshake failed: invalid Sec-WebSocket-Accept');
        }
    }

    /**
     * Builds the TLS context for a `wss://` connection from the client options.
     */
    private function buildTlsContext(string $host): ClientTlsContext
    {
        // Honor a caller-supplied TLS context verbatim (in-memory PEM, ALPN, custom verification).
        if ($this->options->tlsContext !== null) {
            return $this->options->tlsContext;
        }

        $peerName = $this->options->tlsPeerName;
        if ($peerName === null || $peerName === '') {
            $peerName = $host;
        }

        $tlsContext = new ClientTlsContext($peerName);

        if (!$this->options->tlsVerifyPeer) {
            $tlsContext = $tlsContext->withoutPeerVerification();
        }

        if ($this->options->tlsCaFile !== null && $this->options->tlsCaFile !== '') {
            $tlsContext = $tlsContext->withCaFile($this->options->tlsCaFile);
        }

        if ($this->options->tlsCertFile !== null && $this->options->tlsCertFile !== '') {
            $tlsContext = $tlsContext->withCertificate(new Certificate(
                $this->options->tlsCertFile,
                $this->options->tlsKeyFile,
                $this->options->tlsKeyPassphrase,
            ));
        }

        return $tlsContext;
    }
}
