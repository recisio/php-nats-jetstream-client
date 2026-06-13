<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use FilesystemIterator;
use PHPUnit\Framework\TestCase;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use SplFileInfo;

/**
 * Guards #88: production code must derive timeouts/deadlines/elapsed times from the monotonic clock
 * (hrtime, via NatsConnection/SubscriptionQueue::monotonicSeconds()), never from the wall clock
 * (microtime), which can jump forward/backward (NTP step, suspend/resume) and mis-time or flake a
 * wait (#70). This is a regression guard: it fails if any microtime( call (or mention) reappears in
 * src/.
 */
final class MonotonicClockUsageTest extends TestCase
{
    public function testSrcContainsNoWallClockMicrotime(): void
    {
        $srcDir = dirname(__DIR__, 2) . '/src';
        $offenders = [];

        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($srcDir, FilesystemIterator::SKIP_DOTS),
        );

        /** @var SplFileInfo $file */
        foreach ($iterator as $file) {
            if (!$file->isFile() || $file->getExtension() !== 'php') {
                continue;
            }

            $contents = file_get_contents($file->getPathname());
            self::assertNotFalse($contents);

            if (str_contains($contents, 'microtime(')) {
                $offenders[] = $file->getPathname();
            }
        }

        self::assertSame(
            [],
            $offenders,
            'Production code must use hrtime()-based monotonic timing, not microtime(). Offenders: '
                . implode(', ', $offenders),
        );
    }
}
