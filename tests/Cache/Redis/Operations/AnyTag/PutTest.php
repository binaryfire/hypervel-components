<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\AnyTag;

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
 * Tests for the Put operation (union tags).
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
    public function testPutWithTagsUsesLuaScriptInStandardMode(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // Standard mode uses Lua script with evalSha
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false); // Script not cached
        $client->shouldReceive('eval')
            ->once()
            ->withArgs(function ($script, $args, $numKeys) {
                // Verify Lua script contains expected commands
                $this->assertStringContainsString('SETEX', $script);
                $this->assertStringContainsString('HSETEX', $script);
                $this->assertStringContainsString('ZADD', $script);
                $this->assertStringContainsString('SMEMBERS', $script);
                // 2 keys: cache key + reverse index key
                $this->assertSame(2, $numKeys);

                return true;
            })
            ->andReturn(true);

        // Mock smembers for old tags lookup (Lua script uses this internally but we mock the full execution)
        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $result = $redis->anyTagOps()->put()->execute('foo', 'bar', 60, ['users', 'posts']);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithTagsUsesSequentialCommandsInClusterMode(): void
    {
        // Use anonymous mock with RedisCluster type hint for instanceof check
        // but without enforcing method signatures
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

        // Cluster mode expectations
        $clusterClient->shouldReceive('smembers')->once()->andReturn([]);
        $clusterClient->shouldReceive('setex')->once()->with('prefix:foo', 60, serialize('bar'))->andReturn(true);

        // Multi for reverse index
        $clusterClient->shouldReceive('multi')->andReturn($clusterClient);
        $clusterClient->shouldReceive('del')->andReturn($clusterClient);
        $clusterClient->shouldReceive('sadd')->andReturn($clusterClient);
        $clusterClient->shouldReceive('expire')->andReturn($clusterClient);
        $clusterClient->shouldReceive('exec')->andReturn([true, true, true]);

        // HSETEX for tag hashes (2 tags) - use withAnyArgs to bypass type checking
        $clusterClient->shouldReceive('hsetex')->withAnyArgs()->twice()->andReturn(true);

        // ZADD for registry - use withAnyArgs to handle variable args
        $clusterClient->shouldReceive('zadd')->withAnyArgs()->once()->andReturn(2);

        $redis = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix:',
            'default',
            $poolFactory
        );
        $redis->setTagMode('any');

        $result = $redis->anyTagOps()->put()->execute('foo', 'bar', 60, ['users', 'posts']);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithTagsHandlesEmptyTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);
        $client->shouldReceive('eval')
            ->once()
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $result = $redis->anyTagOps()->put()->execute('foo', 'bar', 60, []);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithTagsWithNumericValue(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);
        $client->shouldReceive('eval')
            ->once()
            ->withArgs(function ($script, $args, $numKeys) {
                // Numeric values should be passed as strings in ARGV
                $this->assertIsString($args[2]); // Serialized value position
                return true;
            })
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $result = $redis->anyTagOps()->put()->execute('foo', 42, 60, ['numbers']);
        $this->assertTrue($result);
    }
}
