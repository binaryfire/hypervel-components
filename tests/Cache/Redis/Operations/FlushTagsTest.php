<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations;

use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use RuntimeException;

/**
 * Tests for the FlushTags operation.
 *
 * @internal
 * @coversNothing
 */
class FlushTagsTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testFlushTagsThrowsRuntimeExceptionUntilPhase7(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('flushTags() will be implemented in Phase 7');

        $redis->flushTags(['users', 'posts']);
    }
}
