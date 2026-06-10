<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

/**
 * Parsed server INFO payload describing capabilities and limits.
 */
final class ServerInfo
{
    /**
     * Captures selected server capabilities from the INFO handshake payload.
     *
     * @param string $serverId Unique server identifier from INFO `server_id`.
     * @param string $serverName Human-readable server name from INFO `server_name`.
     * @param string $version Server version string from INFO `version`.
     * @param bool $jetStreamEnabled Whether INFO advertises JetStream support.
     * @param int $maxPayload Maximum accepted payload bytes (`max_payload`) for PUB/HPUB commands.
     * @param bool $headersSupported Whether server supports NATS headers (`headers` capability).
     * @param string|null $nonce Optional nonce challenge from INFO used for JWT/NKey signature auth.
     * @param bool $tlsRequired Whether the server requires a TLS upgrade (`tls_required`).
     * @param bool $tlsAvailable Whether the server offers an optional TLS upgrade (`tls_available`).
     * @param bool $lameDuckMode Whether the server has entered lame-duck mode (`ldm`); set in an async
     *                           INFO update when the server is shutting down gracefully.
     * @param list<string> $connectUrls Additional server endpoints advertised by the cluster
     *                                   (`connect_urls`) for client-side discovery/failover.
     */
    public function __construct(
        public readonly string $serverId,
        public readonly string $serverName,
        public readonly string $version,
        public readonly bool $jetStreamEnabled,
        public readonly int $maxPayload,
        public readonly bool $headersSupported,
        public readonly ?string $nonce = null,
        public readonly bool $tlsRequired = false,
        public readonly bool $tlsAvailable = false,
        public readonly bool $lameDuckMode = false,
        public readonly array $connectUrls = [],
    ) {}

    /**
     * Creates a typed ServerInfo object from raw INFO JSON data.
     *
     * @param array<string,mixed> $payload
     */
    public static function fromInfoPayload(array $payload): self
    {
        $connectUrls = [];
        if (isset($payload['connect_urls']) && is_array($payload['connect_urls'])) {
            foreach ($payload['connect_urls'] as $url) {
                if (is_string($url) && $url !== '') {
                    $connectUrls[] = $url;
                }
            }
        }

        return new self(
            serverId: (string) ($payload['server_id'] ?? ''),
            serverName: (string) ($payload['server_name'] ?? ''),
            version: (string) ($payload['version'] ?? ''),
            jetStreamEnabled: (bool) ($payload['jetstream'] ?? false),
            maxPayload: (int) ($payload['max_payload'] ?? 0),
            headersSupported: (bool) ($payload['headers'] ?? false),
            nonce: isset($payload['nonce']) ? (string) $payload['nonce'] : null,
            tlsRequired: (bool) ($payload['tls_required'] ?? false),
            tlsAvailable: (bool) ($payload['tls_available'] ?? false),
            lameDuckMode: (bool) ($payload['ldm'] ?? false),
            connectUrls: $connectUrls,
        );
    }
}
