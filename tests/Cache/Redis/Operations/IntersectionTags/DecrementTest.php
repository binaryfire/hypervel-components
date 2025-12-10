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
 * Tests for the Decrement operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class DecrementTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testDecrementWithTagsInPipelineMode(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // ZADD NX for tag with score -1 (only add if not exists)
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturn($client);

        // DECRBY
        $client->shouldReceive('decrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, 5]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertSame(5, $result);
    }

    /**
     * @test
     */
    public function testDecrementWithCustomValue(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturn($client);

        $client->shouldReceive('decrby')
            ->once()
            ->with('prefix:counter', 10)
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([0, -5]);  // 0 means key already existed (NX condition)

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            10,
            ['tag:users:entries']
        );

        $this->assertSame(-5, $result);
    }

    /**
     * @test
     */
    public function testDecrementWithMultipleTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // ZADD NX for each tag
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturn($client);
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', ['NX'], -1, 'counter')
            ->andReturn($client);

        $client->shouldReceive('decrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1, 9]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            1,
            ['tag:users:entries', 'tag:posts:entries']
        );

        $this->assertSame(9, $result);
    }

    /**
     * @test
     */
    public function testDecrementWithEmptyTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // No ZADD calls expected
        $client->shouldReceive('decrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([-1]);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            1,
            []
        );

        $this->assertSame(-1, $result);
    }

    /**
     * @test
     */
    public function testDecrementInClusterModeUsesSequentialCommands(): void
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
        $clusterClient->shouldNotReceive('pipeline');

        // Sequential ZADD NX
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', ['NX'], -1, 'counter')
            ->andReturn(1);

        // Sequential DECRBY
        $clusterClient->shouldReceive('decrby')
            ->once()
            ->with('prefix:counter', 1)
            ->andReturn(0);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertSame(0, $result);
    }

    /**
     * @test
     */
    public function testDecrementReturnsFalseOnPipelineFailure(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        $client->shouldReceive('zadd')->andReturn($client);
        $client->shouldReceive('decrby')->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn(false);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->decrement()->execute(
            'counter',
            1,
            ['tag:users:entries']
        );

        $this->assertFalse($result);
    }
}
