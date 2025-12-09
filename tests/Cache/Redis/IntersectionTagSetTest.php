<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Carbon\Carbon;
use Hyperf\Collection\LazyCollection;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\IntersectionTagSet;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Mockery\MockInterface;
use Redis;

/**
 * @internal
 * @coversNothing
 */
class IntersectionTagSetTest extends TestCase
{
    private RedisStore $store;

    private MockInterface|RedisConnection $connection;

    /**
     * Set up test fixtures.
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->mockRedis();
    }

    /**
     * @test
     */
    public function testAddEntryWithTtl(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->connection->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 300, 'mykey')
            ->andReturn(1);

        $tagSet->addEntry('mykey', 300);
    }

    /**
     * @test
     */
    public function testAddEntryWithZeroTtlStoresNegativeOne(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        // TTL of 0 should store -1 (forever)
        $this->connection->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturn(1);

        $tagSet->addEntry('mykey', 0);
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenCondition(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        // With updateWhen='NX', should pass NX flag to zadd
        $this->connection->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', 'NX', -1, 'mykey')
            ->andReturn(1);

        $tagSet->addEntry('mykey', 0, 'NX');
    }

    /**
     * @test
     */
    public function testAddEntryWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $tagSet = new IntersectionTagSet($this->store, ['users', 'posts']);

        // Should add to both tag sets
        $this->connection->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturn(1);
        $this->connection->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', now()->timestamp + 60, 'mykey')
            ->andReturn(1);

        $tagSet->addEntry('mykey', 60);
    }

    /**
     * @test
     */
    public function testEntriesReturnsLazyCollection(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', m::any(), '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return ['key1' => 1, 'key2' => 2];
            });
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', 0, '*', 1000)
            ->andReturnNull();

        $entries = $tagSet->entries();

        $this->assertInstanceOf(LazyCollection::class, $entries);
        $this->assertSame(['key1', 'key2'], $entries->all());
    }

    /**
     * @test
     */
    public function testEntriesWithEmptyTagSetReturnsEmptyCollection(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', m::any(), '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return [];
            });
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', 0, '*', 1000)
            ->andReturnNull();

        $entries = $tagSet->entries();

        $this->assertSame([], $entries->all());
    }

    /**
     * @test
     */
    public function testEntriesWithMultipleTags(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users', 'posts']);

        // First tag
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', m::any(), '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return ['user_key1' => 1, 'user_key2' => 2];
            });
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', 0, '*', 1000)
            ->andReturnNull();

        // Second tag
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:posts:entries', m::any(), '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return ['post_key1' => 1];
            });
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:posts:entries', 0, '*', 1000)
            ->andReturnNull();

        $entries = $tagSet->entries();

        // Should combine entries from both tags
        $this->assertSame(['user_key1', 'user_key2', 'post_key1'], $entries->all());
    }

    /**
     * @test
     */
    public function testEntriesDeduplicatesWithinTag(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', m::any(), '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                // Same key appears with different scores (shouldn't happen but defensive)
                return ['key1' => 1, 'key2' => 2];
            });
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', 0, '*', 1000)
            ->andReturnNull();

        $entries = $tagSet->entries();

        // array_unique is applied
        $this->assertCount(2, $entries->all());
    }

    /**
     * @test
     */
    public function testEntriesHandlesNullScanResult(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        // zScan returns null/false when done
        $this->connection->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:users:entries', m::any(), '*', 1000)
            ->andReturnNull();

        $entries = $tagSet->entries();

        $this->assertSame([], $entries->all());
    }

    /**
     * @test
     */
    public function testFlushStaleEntriesUsesPipeline(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $pipeline = m::mock('Pipeline');
        $pipeline->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:tag:users:entries', '0', (string) now()->timestamp)
            ->andReturnSelf();
        $pipeline->shouldReceive('exec')->once()->andReturn([0]);

        $this->connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $tagSet->flushStaleEntries();
    }

    /**
     * @test
     */
    public function testFlushStaleEntriesWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $tagSet = new IntersectionTagSet($this->store, ['users', 'posts']);

        $pipeline = m::mock('Pipeline');
        $pipeline->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:tag:users:entries', '0', (string) now()->timestamp)
            ->andReturnSelf();
        $pipeline->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:tag:posts:entries', '0', (string) now()->timestamp)
            ->andReturnSelf();
        $pipeline->shouldReceive('exec')->once()->andReturn([0, 0]);

        $this->connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $tagSet->flushStaleEntries();
    }

    /**
     * @test
     */
    public function testFlushTagCallsResetTag(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        // resetTag calls store->forget which uses del
        $this->connection->shouldReceive('del')
            ->once()
            ->with('prefix:tag:users:entries')
            ->andReturn(1);

        $result = $tagSet->flushTag('users');

        // Returns the tag identifier
        $this->assertSame('tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testResetTagDeletesTagAndReturnsId(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->connection->shouldReceive('del')
            ->once()
            ->with('prefix:tag:users:entries')
            ->andReturn(1);

        $result = $tagSet->resetTag('users');

        $this->assertSame('tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testTagIdReturnsCorrectFormat(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        $this->assertSame('tag:users:entries', $tagSet->tagId('users'));
        $this->assertSame('tag:posts:entries', $tagSet->tagId('posts'));
    }

    /**
     * @test
     */
    public function testTagKeyReturnsCorrectFormat(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users']);

        // In IntersectionTagSet, tagKey and tagId return the same value
        $this->assertSame('tag:users:entries', $tagSet->tagKey('users'));
    }

    /**
     * Set up the Redis mocks.
     */
    private function mockRedis(): void
    {
        $this->connection = m::mock(RedisConnection::class);
        $this->connection->shouldReceive('release')->zeroOrMoreTimes();
        $this->connection->shouldReceive('serialized')->andReturn(false)->byDefault();

        $pool = m::mock(RedisPool::class);
        $pool->shouldReceive('get')->andReturn($this->connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        $redisFactory = m::mock(RedisFactory::class);

        $this->store = new RedisStore($redisFactory, 'prefix', 'default', $poolFactory);
    }
}
