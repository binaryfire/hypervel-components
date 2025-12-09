<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\IntersectionTags;

use Carbon\Carbon;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\Operations\IntersectionTags\AddEntry;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;
use RedisCluster;

/**
 * Tests for the AddEntry operation.
 *
 * @internal
 * @coversNothing
 */
class AddEntryTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testAddEntryWithTtl(): void
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
            ->with('prefix:tag:users:entries', now()->timestamp + 300, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 300, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithZeroTtlStoresNegativeOne(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithNegativeTtlStoresNegativeOne(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', -5, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenNxCondition(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries'], 'NX');
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenXxCondition(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['XX'], -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries'], 'XX');
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenGtCondition(): void
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
            ->with('prefix:tag:users:entries', ['GT'], now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, ['tag:users:entries'], 'GT');
    }

    /**
     * @test
     */
    public function testAddEntryWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        // Should use pipeline for multiple tags
        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, ['tag:users:entries', 'tag:posts:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithEmptyTagsArrayDoesNothing(): void
    {
        $connection = $this->mockConnection();
        // No pipeline or zadd calls should be made
        $connection->shouldNotReceive('multi');
        $connection->shouldNotReceive('zadd');

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, []);
    }

    /**
     * @test
     */
    public function testAddEntryUsesCorrectPrefix(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('custom_prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection, 'custom_prefix');
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryClusterModeUsesSequentialCommands(): void
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

        // Should use sequential zadd calls directly on client
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 300, 'mykey')
            ->andReturn(1);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $operation = new AddEntry($store->getContext());
        $operation->execute('mykey', 300, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryClusterModeWithMultipleTags(): void
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

        // Should use sequential zadd calls for each tag
        $expectedScore = now()->timestamp + 60;
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', $expectedScore, 'mykey')
            ->andReturn(1);
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', $expectedScore, 'mykey')
            ->andReturn(1);
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:comments:entries', $expectedScore, 'mykey')
            ->andReturn(1);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $operation = new AddEntry($store->getContext());
        $operation->execute('mykey', 60, ['tag:users:entries', 'tag:posts:entries', 'tag:comments:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryClusterModeWithUpdateWhenFlag(): void
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

        // Should use zadd with NX flag as array (phpredis requires array for options)
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'mykey')
            ->andReturn(1);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $operation = new AddEntry($store->getContext());
        $operation->execute('mykey', 0, ['tag:users:entries'], 'NX');
    }

    /**
     * @test
     */
    public function testAddEntryClusterModeWithZeroTtlStoresNegativeOne(): void
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

        // Score should be -1 for forever items (TTL = 0)
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturn(1);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $operation = new AddEntry($store->getContext());
        $operation->execute('mykey', 0, ['tag:users:entries']);
    }
}
