<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\AllTag;

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
 * Tests for the Forever operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class ForeverTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testForeverStoresValueWithTagsInPipelineMode(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // ZADD for tag with score -1 (forever)
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:_all:tag:users:entries', -1, 'mykey')
            ->andReturn($client);

        // SET for cache value (no expiration)
        $client->shouldReceive('set')
            ->once()
            ->with('prefix:mykey', serialize('myvalue'))
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection);
        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            ['_all:tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForeverWithMultipleTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // ZADD for each tag with score -1
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:_all:tag:users:entries', -1, 'mykey')
            ->andReturn($client);
        $client->shouldReceive('zadd')
            ->once()
            ->with('prefix:_all:tag:posts:entries', -1, 'mykey')
            ->andReturn($client);

        // SET for cache value
        $client->shouldReceive('set')
            ->once()
            ->with('prefix:mykey', serialize('myvalue'))
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1, true]);

        $store = $this->createStore($connection);
        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            ['_all:tag:users:entries', '_all:tag:posts:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForeverWithEmptyTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        // SET for cache value only
        $client->shouldReceive('set')
            ->once()
            ->with('prefix:mykey', serialize('myvalue'))
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([true]);

        $store = $this->createStore($connection);
        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            []
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForeverInClusterModeUsesSequentialCommands(): void
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

        // Sequential ZADD with score -1
        $clusterClient->shouldReceive('zadd')
            ->once()
            ->with('prefix:_all:tag:users:entries', -1, 'mykey')
            ->andReturn(1);

        // Sequential SET
        $clusterClient->shouldReceive('set')
            ->once()
            ->with('prefix:mykey', serialize('myvalue'))
            ->andReturn(true);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix:',
            'default',
            $poolFactory
        );

        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            ['_all:tag:users:entries']
        );

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForeverReturnsFalseOnFailure(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        $client->shouldReceive('zadd')->andReturn($client);
        $client->shouldReceive('set')->andReturn($client);

        // SET returns false (failure)
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, false]);

        $store = $this->createStore($connection);
        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            ['_all:tag:users:entries']
        );

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testForeverUsesCorrectPrefix(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('pipeline')->once()->andReturn($client);

        $client->shouldReceive('zadd')
            ->once()
            ->with('custom:_all:tag:users:entries', -1, 'mykey')
            ->andReturn($client);

        $client->shouldReceive('set')
            ->once()
            ->with('custom:mykey', serialize('myvalue'))
            ->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1, true]);

        $store = $this->createStore($connection, 'custom:');
        $result = $store->allTagOps()->forever()->execute(
            'mykey',
            'myvalue',
            ['_all:tag:users:entries']
        );

        $this->assertTrue($result);
    }
}
