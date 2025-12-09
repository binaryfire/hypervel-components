<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Support;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;
use RedisCluster;
use RuntimeException;

/**
 * @internal
 * @coversNothing
 */
class StoreContextTest extends TestCase
{
    public function testPrefixReturnsConfiguredPrefix(): void
    {
        $context = $this->createContext(prefix: 'myapp:');

        $this->assertSame('myapp:', $context->prefix());
    }

    public function testConnectionNameReturnsConfiguredConnectionName(): void
    {
        $context = $this->createContext(connectionName: 'cache');

        $this->assertSame('cache', $context->connectionName());
    }

    public function testTagPrefixCombinesPrefixWithTagSegment(): void
    {
        $context = $this->createContext(prefix: 'myapp:');

        $this->assertSame('myapp:_erc:tag:', $context->tagPrefix());
    }

    public function testTagHashKeyBuildsCorrectFormat(): void
    {
        $context = $this->createContext(prefix: 'myapp:');

        $this->assertSame('myapp:_erc:tag:users:entries', $context->tagHashKey('users'));
        $this->assertSame('myapp:_erc:tag:posts:entries', $context->tagHashKey('posts'));
    }

    public function testTagHashSuffixReturnsConstant(): void
    {
        $context = $this->createContext();

        $this->assertSame(':entries', $context->tagHashSuffix());
    }

    public function testReverseIndexKeyBuildsCorrectFormat(): void
    {
        $context = $this->createContext(prefix: 'myapp:');

        $this->assertSame('myapp:user:1:_erc:tags', $context->reverseIndexKey('user:1'));
        $this->assertSame('myapp:post:42:_erc:tags', $context->reverseIndexKey('post:42'));
    }

    public function testRegistryKeyBuildsCorrectFormat(): void
    {
        $context = $this->createContext(prefix: 'myapp:');

        $this->assertSame('myapp:_erc:tag:registry', $context->registryKey());
    }

    public function testWithConnectionGetsConnectionFromPoolAndReleasesIt(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);

        $poolFactory->shouldReceive('getPool')
            ->once()
            ->with('default')
            ->andReturn($pool);

        $pool->shouldReceive('get')
            ->once()
            ->andReturn($connection);

        $connection->shouldReceive('release')
            ->once();

        $context = new StoreContext($poolFactory, 'default', 'prefix:');

        $result = $context->withConnection(function ($conn) use ($connection) {
            $this->assertSame($connection, $conn);
            return 'callback-result';
        });

        $this->assertSame('callback-result', $result);
    }

    public function testWithConnectionReleasesConnectionOnException(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);

        $poolFactory->shouldReceive('getPool')
            ->once()
            ->with('default')
            ->andReturn($pool);

        $pool->shouldReceive('get')
            ->once()
            ->andReturn($connection);

        $connection->shouldReceive('release')
            ->once();

        $context = new StoreContext($poolFactory, 'default', 'prefix:');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Test exception');

        $context->withConnection(function () {
            throw new RuntimeException('Test exception');
        });
    }

    public function testIsClusterReturnsTrueForRedisCluster(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(RedisCluster::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');

        $context = new StoreContext($poolFactory, 'default', 'prefix:');

        $this->assertTrue($context->isCluster());
    }

    public function testIsClusterReturnsFalseForRegularRedis(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');

        $context = new StoreContext($poolFactory, 'default', 'prefix:');

        $this->assertFalse($context->isCluster());
    }

    public function testOptPrefixReturnsRedisOptionPrefix(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('redis_prefix:');

        $context = new StoreContext($poolFactory, 'default', 'cache:');

        $this->assertSame('redis_prefix:', $context->optPrefix());
    }

    public function testOptPrefixReturnsEmptyStringWhenNotSet(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn(null);

        $context = new StoreContext($poolFactory, 'default', 'cache:');

        $this->assertSame('', $context->optPrefix());
    }

    public function testFullTagPrefixIncludesOptPrefix(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('redis:');

        $context = new StoreContext($poolFactory, 'default', 'cache:');

        $this->assertSame('redis:cache:_erc:tag:', $context->fullTagPrefix());
    }

    public function testFullReverseIndexKeyIncludesOptPrefix(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('redis:');

        $context = new StoreContext($poolFactory, 'default', 'cache:');

        $this->assertSame('redis:cache:user:1:_erc:tags', $context->fullReverseIndexKey('user:1'));
    }

    public function testFullRegistryKeyIncludesOptPrefix(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('client')->andReturn($client);
        $connection->shouldReceive('release');
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('redis:');

        $context = new StoreContext($poolFactory, 'default', 'cache:');

        $this->assertSame('redis:cache:_erc:tag:registry', $context->fullRegistryKey());
    }

    public function testConstantsHaveExpectedValues(): void
    {
        $this->assertSame('_erc:tag:', StoreContext::TAG_SEGMENT);
        $this->assertSame(':entries', StoreContext::TAG_HASH_SUFFIX);
        $this->assertSame(':_erc:tags', StoreContext::REVERSE_INDEX_SUFFIX);
        $this->assertSame('registry', StoreContext::TAG_REGISTRY_NAME);
        $this->assertSame(253402300799, StoreContext::MAX_EXPIRY);
        $this->assertSame('1', StoreContext::TAG_FIELD_VALUE);
    }

    private function createContext(
        string $connectionName = 'default',
        string $prefix = 'prefix:'
    ): StoreContext {
        $poolFactory = m::mock(PoolFactory::class);

        return new StoreContext($poolFactory, $connectionName, $prefix);
    }
}
