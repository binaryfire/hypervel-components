<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use BadMethodCallException;
use Generator;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\IntersectionTaggedCache;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Cache\Redis\UnionTaggedCache;
use Hypervel\Cache\Redis\UnionTagSet;
use Hypervel\Cache\RedisStore;
use Hypervel\Cache\TaggedCache;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Mockery\MockInterface;

/**
 * @internal
 * @coversNothing
 */
class UnionTaggedCacheTest extends TestCase
{
    private MockInterface|RedisStore $store;

    private MockInterface|StoreContext $context;

    private MockInterface|RedisConnection $connection;

    private UnionTagSet $tagSet;

    /**
     * Set up test fixtures.
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->mockStore();
    }

    /**
     * @test
     */
    public function testIsInstanceOfIntersectionTaggedCache(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->assertInstanceOf(IntersectionTaggedCache::class, $cache);
    }

    /**
     * @test
     */
    public function testIsInstanceOfTaggedCache(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->assertInstanceOf(TaggedCache::class, $cache);
    }

    /**
     * @test
     */
    public function testGetThrowsBadMethodCallException(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('Cannot get items via tags in union mode');

        $cache->get('key');
    }

    /**
     * @test
     */
    public function testManyThrowsBadMethodCallException(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('Cannot get items via tags in union mode');

        $cache->many(['key1', 'key2']);
    }

    /**
     * @test
     */
    public function testHasThrowsBadMethodCallException(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('Cannot check existence via tags in union mode');

        $cache->has('key');
    }

    /**
     * @test
     */
    public function testPullThrowsBadMethodCallException(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('Cannot pull items via tags in union mode');

        $cache->pull('key');
    }

    /**
     * @test
     */
    public function testForgetThrowsBadMethodCallException(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->expectException(BadMethodCallException::class);
        $this->expectExceptionMessage('Cannot forget items via tags in union mode');

        $cache->forget('key');
    }

    /**
     * @test
     */
    public function testPutDelegatesToStorePutWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('putWithTags')
            ->once()
            ->with('mykey', 'myvalue', 60, ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->put('mykey', 'myvalue', 60);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithNullTtlCallsForever(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('foreverWithTags')
            ->once()
            ->with('mykey', 'myvalue', ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->put('mykey', 'myvalue', null);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutWithZeroTtlReturnsFalse(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $result = $cache->put('mykey', 'myvalue', 0);

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testPutWithArrayCallsPutMany(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('putManyWithTags')
            ->once()
            ->with(['key1' => 'value1', 'key2' => 'value2'], 60, ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->put(['key1' => 'value1', 'key2' => 'value2'], 60);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyDelegatesToStorePutManyWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('putManyWithTags')
            ->once()
            ->with(['key1' => 'value1', 'key2' => 'value2'], 120, ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->putMany(['key1' => 'value1', 'key2' => 'value2'], 120);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithNullTtlCallsForeverForEach(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Should call forever for each value
        $this->store->shouldReceive('foreverWithTags')
            ->once()
            ->with('key1', 'value1', ['users', 'posts'])
            ->andReturn(true);
        $this->store->shouldReceive('foreverWithTags')
            ->once()
            ->with('key2', 'value2', ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->putMany(['key1' => 'value1', 'key2' => 'value2'], null);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testPutManyWithZeroTtlReturnsFalse(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $result = $cache->putMany(['key1' => 'value1'], 0);

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testAddDelegatesToStoreAddWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('addWithTags')
            ->once()
            ->with('mykey', 'myvalue', 60, ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->add('mykey', 'myvalue', 60);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testAddWithNullTtlDefaultsToOneYear(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('addWithTags')
            ->once()
            ->with('mykey', 'myvalue', 31536000, ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->add('mykey', 'myvalue', null);

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testAddWithZeroTtlReturnsFalse(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $result = $cache->add('mykey', 'myvalue', 0);

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testForeverDelegatesToStoreForeverWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('foreverWithTags')
            ->once()
            ->with('mykey', 'myvalue', ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->forever('mykey', 'myvalue');

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testIncrementDelegatesToStoreIncrementWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('incrementWithTags')
            ->once()
            ->with('counter', 1, ['users', 'posts'])
            ->andReturn(5);

        $result = $cache->increment('counter');

        $this->assertSame(5, $result);
    }

    /**
     * @test
     */
    public function testIncrementWithCustomValue(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('incrementWithTags')
            ->once()
            ->with('counter', 10, ['users', 'posts'])
            ->andReturn(15);

        $result = $cache->increment('counter', 10);

        $this->assertSame(15, $result);
    }

    /**
     * @test
     */
    public function testDecrementDelegatesToStoreDecrementWithTags(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('decrementWithTags')
            ->once()
            ->with('counter', 1, ['users', 'posts'])
            ->andReturn(3);

        $result = $cache->decrement('counter');

        $this->assertSame(3, $result);
    }

    /**
     * @test
     */
    public function testDecrementWithCustomValue(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('decrementWithTags')
            ->once()
            ->with('counter', 5, ['users', 'posts'])
            ->andReturn(0);

        $result = $cache->decrement('counter', 5);

        $this->assertSame(0, $result);
    }

    /**
     * @test
     */
    public function testFlushDelegatesToTagSetFlush(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('flushTags')
            ->once()
            ->with(['users', 'posts']);

        $result = $cache->flush();

        $this->assertTrue($result);
    }

    /**
     * @test
     */
    public function testItemsDelegatesToStoreTagItems(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $generator = $this->createGenerator(['key1' => 'value1', 'key2' => 'value2']);

        $this->store->shouldReceive('tagItems')
            ->once()
            ->with(['users', 'posts'])
            ->andReturn($generator);

        $result = $cache->items();

        $this->assertInstanceOf(Generator::class, $result);
    }

    /**
     * @test
     */
    public function testRememberRetrievesExistingValueFromStore(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Store returns existing value
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturn('cached_value');

        $result = $cache->remember('mykey', 60, fn () => 'new_value');

        $this->assertSame('cached_value', $result);
    }

    /**
     * @test
     */
    public function testRememberCallsCallbackAndStoresValueWhenMiss(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Store returns null (miss)
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturnNull();

        // Should store the value with tags
        $this->store->shouldReceive('putWithTags')
            ->once()
            ->with('mykey', 'computed_value', 60, ['users', 'posts'])
            ->andReturn(true);

        $callCount = 0;
        $result = $cache->remember('mykey', 60, function () use (&$callCount) {
            $callCount++;

            return 'computed_value';
        });

        $this->assertSame('computed_value', $result);
        $this->assertSame(1, $callCount);
    }

    /**
     * @test
     */
    public function testRememberForeverRetrievesExistingValueFromStore(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Store returns existing value
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturn('cached_value');

        $result = $cache->rememberForever('mykey', fn () => 'new_value');

        $this->assertSame('cached_value', $result);
    }

    /**
     * @test
     */
    public function testRememberForeverCallsCallbackAndStoresValueWhenMiss(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Store returns null (miss)
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturnNull();

        // Should store the value forever with tags
        $this->store->shouldReceive('foreverWithTags')
            ->once()
            ->with('mykey', 'computed_value', ['users', 'posts'])
            ->andReturn(true);

        $result = $cache->rememberForever('mykey', fn () => 'computed_value');

        $this->assertSame('computed_value', $result);
    }

    /**
     * @test
     */
    public function testGetUnionTagsReturnsTagSet(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->assertSame($this->tagSet, $cache->getUnionTags());
    }

    /**
     * @test
     */
    public function testItemKeyReturnsKeyUnchanged(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // In union mode, keys are NOT namespaced by tags
        // Access protected method via remember which uses itemKey
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey') // Should NOT be prefixed with tag namespace
            ->andReturn('value');

        $cache->remember('mykey', 60, fn () => 'fallback');
    }

    /**
     * @test
     */
    public function testIncrementReturnsFalseOnFailure(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('incrementWithTags')
            ->once()
            ->with('counter', 1, ['users', 'posts'])
            ->andReturn(false);

        $result = $cache->increment('counter');

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testDecrementReturnsFalseOnFailure(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('decrementWithTags')
            ->once()
            ->with('counter', 1, ['users', 'posts'])
            ->andReturn(false);

        $result = $cache->decrement('counter');

        $this->assertFalse($result);
    }

    /**
     * @test
     */
    public function testRememberPropagatesExceptionFromCallback(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturnNull();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Callback failed');

        $cache->remember('mykey', 60, function () {
            throw new \RuntimeException('Callback failed');
        });
    }

    /**
     * @test
     */
    public function testRememberForeverPropagatesExceptionFromCallback(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturnNull();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Forever callback failed');

        $cache->rememberForever('mykey', function () {
            throw new \RuntimeException('Forever callback failed');
        });
    }

    /**
     * @test
     */
    public function testRememberDoesNotCallCallbackWhenValueExists(): void
    {
        $cache = new UnionTaggedCache($this->store, $this->tagSet);

        // Store returns existing value
        $this->store->shouldReceive('get')
            ->once()
            ->with('mykey')
            ->andReturn('cached_value');

        $callCount = 0;
        $result = $cache->remember('mykey', 60, function () use (&$callCount) {
            $callCount++;

            return 'new_value';
        });

        $this->assertSame('cached_value', $result);
        $this->assertSame(0, $callCount, 'Callback should not be called when cache hit');
    }

    /**
     * Create a generator from an array for testing.
     */
    private function createGenerator(array $items): Generator
    {
        foreach ($items as $key => $value) {
            yield $key => $value;
        }
    }

    /**
     * Set up the store mock.
     */
    private function mockStore(): void
    {
        $this->connection = m::mock(RedisConnection::class);
        $this->connection->shouldReceive('release')->zeroOrMoreTimes();
        $this->connection->shouldReceive('serialized')->andReturn(false)->byDefault();

        $pool = m::mock(RedisPool::class);
        $pool->shouldReceive('get')->andReturn($this->connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        $this->context = m::mock(StoreContext::class);

        // Create a partial mock of RedisStore to allow stubbing methods that don't exist yet
        $this->store = m::mock(RedisStore::class)->makePartial();
        $this->store->shouldReceive('getContext')->andReturn($this->context);
        $this->store->shouldReceive('getPrefix')->andReturn('prefix:');

        // Create the tag set with the mocked store
        $this->tagSet = new UnionTagSet($this->store, ['users', 'posts']);
    }
}
