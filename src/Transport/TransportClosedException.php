<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

/**
 * Thrown by a transport's readLine() when the peer closes the connection (EOF) on a live socket.
 *
 * This is deliberately distinct from a read timeout (which surfaces as an Amp CancelledException)
 * and from "no bytes available yet" (an empty string), so the connection layer can tell a genuine
 * peer close apart from an idle read and trigger reconnection from the read path.
 *
 * It extends \RuntimeException (a \Throwable) so the existing catch (\Throwable) recovery paths in
 * NatsConnection handle it without changes, while the catch (CancelledException) timeout paths do
 * not swallow it.
 */
final class TransportClosedException extends \RuntimeException {}
