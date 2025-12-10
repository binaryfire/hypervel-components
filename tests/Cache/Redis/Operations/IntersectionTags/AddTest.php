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
 * Tests for the Add operation (intersection tags).
 *
 * @internal
 * @coversNothing
 */
class AddTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testAddWithTagsReturnsTrueWhenKeyAdded(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $client = $connection->_mockClient;
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        // ZADD for tag with TTL score
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        // Lua script for atomic add
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);

        $client->shouldReceive('eval')
            ->once()
            ->withArgs(function ($script, $args, $numKeys) {
                $this->assertStringContainsString('exists', $script);
                $this->assertStringContainsString('setex', $script);
                $this->assertSame(1, $numKeys);
                $this->assertSame('prefix:mykey', $args[0]);

                return true;
            })
            ->andReturn('OK');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddWithTagsReturnsFalseWhenKeyExists(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $client = $connection->_mockClient;
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('exec')->andReturn([1]);

        // Lua script returns false when key exists
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);
        $client->shouldReceive('eval')
            ->once()
            ->andReturn(false);

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $client = $connection->_mockClient;
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

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1]);

        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn('OK');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddWithEmptyTagsSkipsPipeline(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // No pipeline operations for empty tags
        $connection->shouldNotReceive('multi');

        // Only Lua script for add
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn('OK');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddInClusterModeUsesSequentialCommands(): void
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

        // Lua script
        $clusterClient->shouldReceive('evalSha')
            ->once()
            ->andReturn('OK');

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );

        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddUsesEvalWhenEvalShaFails(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $client = $connection->_mockClient;
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')->andReturnSelf();
        $pipeline->shouldReceive('exec')->andReturn([1]);

        // evalSha fails (script not cached)
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);

        // Falls back to eval
        $client->shouldReceive('eval')
            ->once()
            ->andReturn('OK');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
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
    public function testAddEnforcesMinimumTtlOfOne(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // No pipeline for empty tags
        $connection->shouldNotReceive('multi');

        // TTL should be at least 1
        $client->shouldReceive('evalSha')
            ->once()
            ->withArgs(function ($hash, $args, $numKeys) {
                $this->assertSame(1, $args[2]); // TTL argument

                return true;
            })
            ->andReturn('OK');

        $store = $this->createStore($connection);
        $result = $store->intersectionTagOps()->add()->execute(
            'mykey',
            'myvalue',
            0,  // Zero TTL
            []
        );

        $this->assertTrue($result);
    }
}
