<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations;

use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;

/**
 * Tests for the Forget operation.
 *
 * @internal
 * @coversNothing
 */
class ForgetTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testForgetMethodProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:foo')->andReturn(1);

        $redis = $this->createStore($connection);
        $result = $redis->forget('foo');
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForgetReturnsTrue(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:foo')->andReturn(1);

        $redis = $this->createStore($connection);
        $this->assertTrue($redis->forget('foo'));
    }

    /**
     * @test
     */
    public function testForgetNonExistentKeyReturnsTrue(): void
    {
        // Redis del() returns 0 when key doesn't exist, but we cast to bool
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:nonexistent')->andReturn(0);

        $redis = $this->createStore($connection);
        // Should still return true (key is "forgotten" even if it didn't exist)
        $this->assertFalse($redis->forget('nonexistent'));
    }
}
