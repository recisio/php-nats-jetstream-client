<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection\Enum;

/**
 * Lifecycle transitions reported to a {@see \IDCT\NATS\Connection\NatsOptions::$connectionListener}.
 *
 * Mirrors the event sets of nats.go (Connect/Disconnect/Reconnect/Closed/DiscoveredServers/LameDuck
 * handlers) and nats.java (`ConnectionListener.Events`).
 */
enum ConnectionEvent
{
    /** The initial connection handshake completed and the connection is open. */
    case Connected;

    /** The transport was lost; the client will attempt to reconnect (if enabled). */
    case Disconnected;

    /** A reconnect attempt succeeded and subscriptions were replayed. */
    case Reconnected;

    /** The connection was closed permanently (explicit disconnect or reconnect attempts exhausted). */
    case Closed;

    /** The server advertised additional cluster endpoints in an async INFO (`connect_urls`). */
    case DiscoveredServers;

    /** The server entered lame-duck mode (`ldm`) and is shutting down gracefully. */
    case LameDuck;
}
