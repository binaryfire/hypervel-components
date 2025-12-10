<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Concerns;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Mockery as m;
use Redis;
use RedisCluster;

/**
 * Shared test infrastructure for Redis cache operation tests.
 *
 * Provides helper methods for mocking Redis connections, pool factories,
 * and creating RedisStore instances for testing.
 */
trait MocksRedisConnections
{
    /**
     * Create a mock RedisConnection with standard expectations.
     *
     * By default creates a mock with a standard Redis client (not cluster).
     * Use mockClusterConnection() for cluster mode tests.
     *
     * We use an anonymous mock for the client (not m::mock(Redis::class))
     * because mocking the native phpredis extension class can cause
     * unexpected fallthrough to real Redis connections when expectations
     * don't match.
     */
    protected function mockConnection(): m\MockInterface|RedisConnection
    {
        // Anonymous mock - not bound to Redis extension class
        // This prevents fallthrough to real Redis when expectations don't match
        $client = m::mock();
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE)
            ->byDefault();
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('')
            ->byDefault();

        // Default pipeline() returns self for chaining (can be overridden in tests)
        $client->shouldReceive('pipeline')->andReturn($client)->byDefault();
        $client->shouldReceive('exec')->andReturn([])->byDefault();

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false)->byDefault();
        $connection->shouldReceive('client')->andReturn($client)->byDefault();

        // Store client reference for tests that need to set expectations on it
        $connection->_mockClient = $client;

        return $connection;
    }

    /**
     * Create a mock RedisConnection configured as a cluster connection.
     *
     * The client mock is configured to pass instanceof RedisCluster checks
     * which triggers cluster mode in PutMany (uses multi() instead of Lua).
     */
    protected function mockClusterConnection(): m\MockInterface|RedisConnection
    {
        // Mock that identifies as RedisCluster for instanceof checks
        $client = m::mock(RedisCluster::class);
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE)
            ->byDefault();

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false)->byDefault();
        $connection->shouldReceive('client')->andReturn($client)->byDefault();

        // Store client reference for tests that need to set expectations on it
        $connection->_mockClient = $client;

        return $connection;
    }

    /**
     * Create a PoolFactory mock that returns the given connection.
     */
    protected function createPoolFactory(
        m\MockInterface|RedisConnection $connection,
        string $connectionName = 'default'
    ): m\MockInterface|PoolFactory {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);

        $poolFactory->shouldReceive('getPool')
            ->with($connectionName)
            ->andReturn($pool);

        $pool->shouldReceive('get')->andReturn($connection);

        return $poolFactory;
    }

    /**
     * Create a RedisStore with a mocked connection.
     */
    protected function createStore(
        m\MockInterface|RedisConnection $connection,
        string $prefix = 'prefix',
        string $connectionName = 'default'
    ): RedisStore {
        return new RedisStore(
            m::mock(RedisFactory::class),
            $prefix,
            $connectionName,
            $this->createPoolFactory($connection, $connectionName)
        );
    }
}
