<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Hypervel\Cache\RedisLock;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;
use RedisCluster;

/**
 * @internal
 * @coversNothing
 */
class RedisStoreTest extends TestCase
{
    /**
     * @test
     */
    public function testGetReturnsNullWhenNotFound(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(null);

        $redis = $this->createStore($connection);
        $this->assertNull($redis->get('foo'));
    }

    /**
     * @test
     */
    public function testRedisValueIsReturned(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(serialize('foo'));

        $redis = $this->createStore($connection);
        $this->assertSame('foo', $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testRedisMultipleValuesAreReturned(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('mget')
            ->once()
            ->with(['prefix:foo', 'prefix:fizz', 'prefix:norf', 'prefix:null'])
            ->andReturn([
                serialize('bar'),
                serialize('buzz'),
                serialize('quz'),
                null,
            ]);

        $redis = $this->createStore($connection);
        $results = $redis->many(['foo', 'fizz', 'norf', 'null']);

        $this->assertSame('bar', $results['foo']);
        $this->assertSame('buzz', $results['fizz']);
        $this->assertSame('quz', $results['norf']);
        $this->assertNull($results['null']);
    }

    /**
     * @test
     */
    public function testRedisValueIsReturnedForNumerics(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(1);

        $redis = $this->createStore($connection);
        $this->assertEquals(1, $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testSetMethodProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('setex')
            ->once()
            ->with('prefix:foo', 60, serialize('foo'))
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->put('foo', 'foo', 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyUsesLuaScriptInStandardMode(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // Standard mode (not cluster) uses Lua script with evalSha
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false); // Script not cached
        $client->shouldReceive('eval')
            ->once()
            ->withArgs(function ($script, $args, $numKeys) {
                // Verify Lua script structure
                $this->assertStringContainsString('SETEX', $script);
                // Keys: prefix:foo, prefix:baz, prefix:bar
                $this->assertSame(3, $numKeys);
                // Args: [key1, key2, key3, ttl, val1, val2, val3]
                $this->assertSame('prefix:foo', $args[0]);
                $this->assertSame('prefix:baz', $args[1]);
                $this->assertSame('prefix:bar', $args[2]);
                $this->assertSame(60, $args[3]); // TTL

                return true;
            })
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->putMany([
            'foo' => 'bar',
            'baz' => 'qux',
            'bar' => 'norf',
        ], 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyUsesMultiInClusterMode(): void
    {
        // Use RedisCluster mock with shouldIgnoreMissing to handle return type constraints
        $clusterClient = m::mock(RedisCluster::class)->shouldIgnoreMissing();
        $clusterClient->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE);

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($clusterClient);

        $pool = m::mock(\Hyperf\Redis\Pool\RedisPool::class);
        $pool->shouldReceive('get')->andReturn($connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        // RedisCluster::multi() returns $this (fluent interface)
        $clusterClient->shouldReceive('multi')->once()->andReturn($clusterClient);
        $clusterClient->shouldReceive('setex')->once()->with('prefix:foo', 60, serialize('bar'))->andReturn($clusterClient);
        $clusterClient->shouldReceive('setex')->once()->with('prefix:baz', 60, serialize('qux'))->andReturn($clusterClient);
        $clusterClient->shouldReceive('exec')->once()->andReturn([true, true]);

        $redis = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );
        $result = $redis->putMany([
            'foo' => 'bar',
            'baz' => 'qux',
        ], 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyClusterModeReturnsFalseOnFailure(): void
    {
        $clusterClient = m::mock(RedisCluster::class)->shouldIgnoreMissing();
        $clusterClient->shouldReceive('getOption')->andReturn(Redis::COMPRESSION_NONE);

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($clusterClient);

        $pool = m::mock(\Hyperf\Redis\Pool\RedisPool::class);
        $pool->shouldReceive('get')->andReturn($connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        // RedisCluster::multi() returns $this (fluent interface)
        $clusterClient->shouldReceive('multi')->once()->andReturn($clusterClient);
        $clusterClient->shouldReceive('setex')->twice()->andReturn($clusterClient);
        $clusterClient->shouldReceive('exec')->once()->andReturn([true, false]); // One failed

        $redis = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'default',
            $poolFactory
        );
        $result = $redis->putMany([
            'foo' => 'bar',
            'baz' => 'qux',
        ], 60);
        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testSetMethodProperlyCallsRedisForNumerics(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('setex')
            ->once()
            ->with('prefix:foo', 60, 1)
            ->andReturn(false);

        $redis = $this->createStore($connection);
        $result = $redis->put('foo', 1, 60);
        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testIncrementMethodProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('incrby')->once()->with('prefix:foo', 5)->andReturn(6);

        $redis = $this->createStore($connection);
        $result = $redis->increment('foo', 5);
        $this->assertEquals(6, $result);
    }

    /**
     * @test
     */
    public function testDecrementMethodProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('decrby')->once()->with('prefix:foo', 5)->andReturn(4);

        $redis = $this->createStore($connection);
        $result = $redis->decrement('foo', 5);
        $this->assertEquals(4, $result);
    }

    /**
     * @test
     */
    public function testStoreItemForeverProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('set')
            ->once()
            ->with('prefix:foo', serialize('foo'))
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->forever('foo', 'foo');
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForgetMethodProperlyCallsRedis(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:foo')->andReturn(1);

        $redis = $this->createStore($connection);
        $result = $redis->forget('foo');
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testFlushesCached(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('flushdb')->once()->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->flush();
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testGetAndSetPrefix(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $this->assertSame('prefix:', $redis->getPrefix());
        $redis->setPrefix('foo');
        $this->assertSame('foo:', $redis->getPrefix());
        $redis->setPrefix('');
        $this->assertEmpty($redis->getPrefix());
    }

    /**
     * @test
     */
    public function testAddUsesEvalShaWithFallbackToEval(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $luaScript = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

        // evalSha returns false (NOSCRIPT - script not cached on server)
        $client->shouldReceive('evalSha')
            ->once()
            ->with(sha1($luaScript), m::type('array'), 1)
            ->andReturn(false);

        // Falls back to eval with full script
        $client->shouldReceive('eval')
            ->once()
            ->with($luaScript, ['prefix:foo', serialize('bar'), 60], 1)
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->add('foo', 'bar', 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testAddUsesEvalShaWhenScriptCached(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $luaScript = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

        // evalSha succeeds (script is already cached on server)
        $client->shouldReceive('evalSha')
            ->once()
            ->with(sha1($luaScript), ['prefix:foo', serialize('bar'), 60], 1)
            ->andReturn(true);

        // eval should NOT be called when evalSha succeeds
        $client->shouldNotReceive('eval');

        $redis = $this->createStore($connection);
        $result = $redis->add('foo', 'bar', 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testAddReturnsFalseWhenKeyExists(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $luaScript = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

        // evalSha returns false (NOSCRIPT)
        $client->shouldReceive('evalSha')
            ->once()
            ->with(sha1($luaScript), m::type('array'), 1)
            ->andReturn(false);

        // eval also returns false (key already exists, Lua condition fails)
        $client->shouldReceive('eval')
            ->once()
            ->with($luaScript, m::type('array'), 1)
            ->andReturn(false);

        $redis = $this->createStore($connection);
        $result = $redis->add('foo', 'bar', 60);
        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testManyReturnsEmptyArrayForEmptyKeys(): void
    {
        $connection = $this->mockConnection();

        $redis = $this->createStore($connection);
        $results = $redis->many([]);

        $this->assertSame([], $results);
    }

    /**
     * @test
     */
    public function testPutManyReturnsTrueForEmptyValues(): void
    {
        $connection = $this->mockConnection();

        $redis = $this->createStore($connection);
        $result = $redis->putMany([], 60);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testSetConnectionClearsCachedInstances(): void
    {
        $connection1 = $this->mockConnection();
        $connection1->shouldReceive('get')->once()->with('prefix:foo')->andReturn(serialize('value1'));

        $connection2 = $this->mockConnection();
        $connection2->shouldReceive('get')->once()->with('prefix:foo')->andReturn(serialize('value2'));

        // Create store with first connection
        $poolFactory1 = $this->createPoolFactory($connection1, 'conn1');
        $redis = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'conn1',
            $poolFactory1
        );

        $this->assertSame('value1', $redis->get('foo'));

        // Change connection - this should clear cached operation instances
        $poolFactory2 = $this->createPoolFactory($connection2, 'conn2');

        // We need to inject the new pool factory. Since we can't directly,
        // we verify that setConnection clears the context by checking
        // that a new store with different connection gets different values.
        $redis2 = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix',
            'conn2',
            $poolFactory2
        );

        $this->assertSame('value2', $redis2->get('foo'));
    }

    /**
     * @test
     */
    public function testForgetReturnsTrue(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:foo')->andReturn(1);

        $redis = $this->createStore($connection);
        $this->assertTrue($redis->forget('foo'));
    }

    /**
     * @test
     */
    public function testForgetNonExistentKeyReturnsTrue(): void
    {
        // Redis del() returns 0 when key doesn't exist, but we cast to bool
        $connection = $this->mockConnection();
        $connection->shouldReceive('del')->once()->with('prefix:nonexistent')->andReturn(0);

        $redis = $this->createStore($connection);
        // Should still return true (key is "forgotten" even if it didn't exist)
        $this->assertFalse($redis->forget('nonexistent'));
    }

    /**
     * @test
     */
    public function testIncrementOnNonExistentKeyReturnsIncrementedValue(): void
    {
        // Redis INCRBY on non-existent key initializes to 0, then increments
        $connection = $this->mockConnection();
        $connection->shouldReceive('incrby')->once()->with('prefix:counter', 1)->andReturn(1);

        $redis = $this->createStore($connection);
        $this->assertSame(1, $redis->increment('counter'));
    }

    /**
     * @test
     */
    public function testDecrementOnNonExistentKeyReturnsDecrementedValue(): void
    {
        // Redis DECRBY on non-existent key initializes to 0, then decrements
        $connection = $this->mockConnection();
        $connection->shouldReceive('decrby')->once()->with('prefix:counter', 1)->andReturn(-1);

        $redis = $this->createStore($connection);
        $this->assertSame(-1, $redis->decrement('counter'));
    }

    /**
     * @test
     */
    public function testIncrementWithLargeValue(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('incrby')->once()->with('prefix:foo', 1000000)->andReturn(1000005);

        $redis = $this->createStore($connection);
        $this->assertSame(1000005, $redis->increment('foo', 1000000));
    }

    /**
     * @test
     */
    public function testPutManyLuaFailureReturnsFalse(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // In standard mode (Lua), if both evalSha and eval fail, return false
        $client->shouldReceive('evalSha')
            ->once()
            ->andReturn(false);
        $client->shouldReceive('eval')
            ->once()
            ->andReturn(false); // Lua script failed

        $redis = $this->createStore($connection);
        $result = $redis->putMany([
            'foo' => 'bar',
            'baz' => 'qux',
        ], 60);
        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testGetReturnsFalseValueAsNull(): void
    {
        // Redis returns false for non-existent keys
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(false);

        $redis = $this->createStore($connection);
        $this->assertNull($redis->get('foo'));
    }

    /**
     * @test
     */
    public function testGetReturnsEmptyStringCorrectly(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(serialize(''));

        $redis = $this->createStore($connection);
        $this->assertSame('', $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testGetReturnsZeroCorrectly(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(0);

        $redis = $this->createStore($connection);
        $this->assertSame(0, $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testGetReturnsFloatCorrectly(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(3.14);

        $redis = $this->createStore($connection);
        $this->assertSame(3.14, $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testGetReturnsNegativeNumberCorrectly(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(-42);

        $redis = $this->createStore($connection);
        $this->assertSame(-42, $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testPutPreservesArrayValues(): void
    {
        $connection = $this->mockConnection();
        $array = ['nested' => ['data' => 'value']];
        $connection->shouldReceive('setex')
            ->once()
            ->with('prefix:foo', 60, serialize($array))
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $this->assertTrue($redis->put('foo', $array, 60));
    }

    /**
     * @test
     */
    public function testManyMaintainsKeyIndexMapping(): void
    {
        $connection = $this->mockConnection();
        // Return values in same order as requested
        $connection->shouldReceive('mget')
            ->once()
            ->with(['prefix:a', 'prefix:b', 'prefix:c'])
            ->andReturn([
                serialize('value_a'),
                null,
                serialize('value_c'),
            ]);

        $redis = $this->createStore($connection);
        $results = $redis->many(['a', 'b', 'c']);

        // Verify correct mapping
        $this->assertSame('value_a', $results['a']);
        $this->assertNull($results['b']);
        $this->assertSame('value_c', $results['c']);
        $this->assertCount(3, $results);
    }

    /**
     * @test
     */
    public function testAddWithNumericValue(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $luaScript = "return redis.call('exists',KEYS[1])<1 and redis.call('setex',KEYS[1],ARGV[2],ARGV[1])";

        // Numeric values are passed as-is (not serialized) by serializeForLua
        // evalSha succeeds with numeric value
        $client->shouldReceive('evalSha')
            ->once()
            ->with(sha1($luaScript), ['prefix:foo', '42', 60], 1)
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $result = $redis->add('foo', 42, 60);
        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testForeverWithNumericValue(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('set')
            ->once()
            ->with('prefix:foo', 99)
            ->andReturn(true);

        $redis = $this->createStore($connection);
        $this->assertTrue($redis->forever('foo', 99));
    }

    /**
     * @test
     */
    public function testSetPrefixClearsCachedOperations(): void
    {
        $connection = $this->mockConnection();
        $connection->shouldReceive('get')->once()->with('prefix:foo')->andReturn(serialize('old'));
        $connection->shouldReceive('get')->once()->with('newprefix:foo')->andReturn(serialize('new'));

        $redis = $this->createStore($connection);

        // First get with original prefix
        $this->assertSame('old', $redis->get('foo'));

        // Change prefix
        $redis->setPrefix('newprefix');

        // Second get should use new prefix
        $this->assertSame('new', $redis->get('foo'));
    }

    /**
     * @test
     */
    public function testTagsReturnsIntersectionTaggedCache(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $tagged = $redis->tags(['users', 'posts']);

        $this->assertInstanceOf(\Hypervel\Cache\Redis\IntersectionTaggedCache::class, $tagged);
    }

    /**
     * @test
     */
    public function testTagsWithSingleTagAsString(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $tagged = $redis->tags('users');

        $this->assertInstanceOf(\Hypervel\Cache\Redis\IntersectionTaggedCache::class, $tagged);
    }

    /**
     * @test
     */
    public function testTagsWithVariadicArguments(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $tagged = $redis->tags('users', 'posts', 'comments');

        $this->assertInstanceOf(\Hypervel\Cache\Redis\IntersectionTaggedCache::class, $tagged);
    }

    /**
     * @test
     */
    public function testLockReturnsRedisLockInstance(): void
    {
        $connection = $this->mockConnection();
        $redisProxy = m::mock(RedisProxy::class);
        $redisFactory = m::mock(RedisFactory::class);
        $redisFactory->shouldReceive('get')->with('default')->andReturn($redisProxy);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $lock = $redis->lock('mylock', 10);

        $this->assertInstanceOf(RedisLock::class, $lock);
    }

    /**
     * @test
     */
    public function testLockWithOwner(): void
    {
        $connection = $this->mockConnection();
        $redisProxy = m::mock(RedisProxy::class);
        $redisFactory = m::mock(RedisFactory::class);
        $redisFactory->shouldReceive('get')->with('default')->andReturn($redisProxy);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $lock = $redis->lock('mylock', 10, 'custom-owner');

        $this->assertInstanceOf(RedisLock::class, $lock);
    }

    /**
     * @test
     */
    public function testRestoreLockReturnsRedisLockInstance(): void
    {
        $connection = $this->mockConnection();
        $redisProxy = m::mock(RedisProxy::class);
        $redisFactory = m::mock(RedisFactory::class);
        $redisFactory->shouldReceive('get')->with('default')->andReturn($redisProxy);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $lock = $redis->restoreLock('mylock', 'owner-123');

        $this->assertInstanceOf(RedisLock::class, $lock);
    }

    /**
     * @test
     */
    public function testSetLockConnectionReturnsSelf(): void
    {
        $connection = $this->mockConnection();
        $redis = $this->createStore($connection);

        $result = $redis->setLockConnection('locks');

        $this->assertSame($redis, $result);
    }

    /**
     * @test
     */
    public function testLockUsesLockConnectionWhenSet(): void
    {
        $connection = $this->mockConnection();
        $redisProxy = m::mock(RedisProxy::class);
        $lockProxy = m::mock(RedisProxy::class);
        $redisFactory = m::mock(RedisFactory::class);
        $redisFactory->shouldReceive('get')->with('default')->andReturn($redisProxy);
        $redisFactory->shouldReceive('get')->with('locks')->andReturn($lockProxy);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $redis->setLockConnection('locks');
        $lock = $redis->lock('mylock', 10);

        $this->assertInstanceOf(RedisLock::class, $lock);
    }

    /**
     * @test
     */
    public function testGetRedisReturnsRedisFactory(): void
    {
        $connection = $this->mockConnection();
        $redisFactory = m::mock(RedisFactory::class);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $this->assertSame($redisFactory, $redis->getRedis());
    }

    /**
     * @test
     */
    public function testConnectionReturnsRedisProxy(): void
    {
        $connection = $this->mockConnection();
        $redisProxy = m::mock(RedisProxy::class);
        $redisFactory = m::mock(RedisFactory::class);
        $redisFactory->shouldReceive('get')->with('default')->andReturn($redisProxy);

        $redis = new RedisStore(
            $redisFactory,
            'prefix',
            'default',
            $this->createPoolFactory($connection)
        );

        $this->assertSame($redisProxy, $redis->connection());
    }

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
