<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\IntersectionTags;

use Carbon\Carbon;
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
 * Tests for the Put operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class PutTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testPutStoresValueWithTagsInPipelineMode(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // ZADD for tag
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        // SETEX for cache value
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 60, serialize('myvalue'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            60,
            ['tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $expectedScore = now()->timestamp + 120;

        // ZADD for each tag
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'mykey')
            ->andReturnSelf();
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', $expectedScore, 'mykey')
            ->andReturnSelf();

        // SETEX for cache value
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 120, serialize('myvalue'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            120,
            ['tag:users:entries', 'tag:posts:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithEmptyTagsStillStoresValue(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // No ZADD calls expected
        // SETEX for cache value
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 60, serialize('myvalue'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            60,
            []
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutUsesCorrectPrefix(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('custom:tag:users:entries', now()->timestamp + 30, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('setex')
            ->once()
            ->with('custom:mykey', 30, serialize('myvalue'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection, 'custom');
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            30,
            ['tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutReturnsFalseOnFailure(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('setex')->andReturnSelf();

        // SETEX returns false (failure)
        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, false]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            60,
            ['tag:users:entries']
        );

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testPutInClusterModeUsesSequentialCommands(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

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

        // Sequential ZADD
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturn(1);

        // Sequential SETEX
        $clusterClient->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 60, serialize('myvalue'))
            ->andReturn(true);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            60,
            ['tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutEnforcesMinimumTtlOfOne(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();

        // TTL should be at least 1
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 1, serialize('myvalue'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            'myvalue',
            0,  // Zero TTL
            ['tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithNumericValue(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();

        // Numeric values are NOT serialized (optimization)
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:mykey', 60, 42)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->put()->execute(
            'mykey',
            42,
            60,
            ['tag:users:entries']
        );

        $this->assertTrue($result);
    }
}
