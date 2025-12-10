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
 * Tests for the PutMany operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class PutManyTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testPutManyWithTagsInPipelineMode(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $expectedScore = now()->timestamp + 60;

        // ZADD for first key to tag
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'ns:foo')
            ->andReturnSelf();

        // SETEX for first key
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:foo', 60, serialize('bar'))
            ->andReturnSelf();

        // ZADD for second key to tag
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'ns:baz')
            ->andReturnSelf();

        // SETEX for second key
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:baz', 60, serialize('qux'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true, 1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar', 'baz' => 'qux'],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $expectedScore = now()->timestamp + 120;

        // ZADD for first key to both tags
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'ns:foo')
            ->andReturnSelf();
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', $expectedScore, 'ns:foo')
            ->andReturnSelf();

        // SETEX for first key
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:foo', 120, serialize('bar'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar'],
            120,
            ['tag:users:entries', 'tag:posts:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithEmptyTags(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // Only SETEX, no ZADD
        $pipeline->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:foo', 60, serialize('bar'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar'],
            60,
            [],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithEmptyValuesReturnsTrue(): void
    {
        $connection = $this->mockConnection();

        // No pipeline operations for empty values
        $connection->shouldNotReceive('multi');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            [],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyInClusterModeUsesSequentialCommands(): void
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

        $expectedScore = now()->timestamp + 60;

        // Sequential ZADD for first key
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'ns:foo')
            ->andReturn(1);

        // Sequential SETEX for first key
        $clusterClient->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:foo', 60, serialize('bar'))
            ->andReturn(true);

        // Sequential ZADD for second key
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'ns:baz')
            ->andReturn(1);

        // Sequential SETEX for second key
        $clusterClient->shouldReceive('setex')
            ->once()
            ->with('prefix:ns:baz', 60, serialize('qux'))
            ->andReturn(true);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar', 'baz' => 'qux'],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyReturnsFalseOnFailure(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('setex')->andReturnSelf();

        // One SETEX fails
        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true, 1, false]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar', 'baz' => 'qux'],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testPutManyReturnsFalseOnPipelineFailure(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('setex')->andReturnSelf();

        // Pipeline fails entirely
        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn(false);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar'],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testPutManyEnforcesMinimumTtlOfOne(): void
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
            ->with('prefix:ns:foo', 1, serialize('bar'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar'],
            0,  // Zero TTL
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithNumericValues(): void
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
            ->with('prefix:ns:count', 60, 42)
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['count' => 42],
            60,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyUsesCorrectPrefix(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $expectedScore = now()->timestamp + 30;

        // Custom prefix should be used
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('custom:tag:users:entries', $expectedScore, 'ns:foo')
            ->andReturnSelf();

        $pipeline->shouldReceive('setex')
            ->once()
            ->with('custom:ns:foo', 30, serialize('bar'))
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection, 'custom');
        $result = $store->intersectionTagOps()->putMany()->execute(
            ['foo' => 'bar'],
            30,
            ['tag:users:entries'],
            'ns:'
        );

        $this->assertTrue($result);
    }
}
