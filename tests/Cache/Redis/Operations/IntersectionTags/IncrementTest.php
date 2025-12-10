<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\IntersectionTags;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;
use RedisCluster;

/**
 * Tests for the Increment operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class IncrementTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testIncrementWithTagsInPipelineMode(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // ZADD NX for tag with score -1 (only add if not exists)
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturnSelf();

        // INCRBY
        $pipeline->shouldReceive('incrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 5]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertSame(5, $result);
    }

    /**
     * @test
     */
    public function testIncrementWithCustomValue(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturnSelf();

        $pipeline->shouldReceive('incrby')
            ->once()
            ->with('prefix:counter', 10)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([0, 15]);  // 0 means key already existed (NX condition)

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            10,
            ['tag:users:entries']
        );

        $this->assertSame(15, $result);
    }

    /**
     * @test
     */
    public function testIncrementWithMultipleTags(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // ZADD NX for each tag
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturnSelf();
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', ['NX'], -1, 'counter')
            ->andReturnSelf();

        $pipeline->shouldReceive('incrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1, 1]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            1,
            ['tag:users:entries', 'tag:posts:entries']
        );

        $this->assertSame(1, $result);
    }

    /**
     * @test
     */
    public function testIncrementWithEmptyTags(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // No ZADD calls expected
        $pipeline->shouldReceive('incrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            1,
            []
        );

        $this->assertSame(1, $result);
    }

    /**
     * @test
     */
    public function testIncrementInClusterModeUsesSequentialCommands(): void
    {
        $clusterClient = m::mock(RedisCluster::class);
        $clusterClient->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE);
        $clusterClient->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('');

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($clusterClient);

        $pool = m::mock(RedisPool::class);
        $pool->shouldReceive('get')->andReturn($connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        // Should NOT use pipeline in cluster mode
        $connection->shouldNotReceive('multi');

        // Sequential ZADD NX
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturn(1);

        // Sequential INCRBY
        $clusterClient->shouldReceive('incrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturn(10);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertSame(10, $result);
    }

    /**
     * @test
     */
    public function testIncrementReturnsFalseOnPipelineFailure(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('incrby')->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn(false);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->increment()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertFalse($result);
    }
}
