<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\IntersectionTagSet;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Mockery\MockInterface;

/**
 * Tests for IntersectionTagSet class.
 *
 * Note: Operation-specific tests (addEntry, entries, flushStaleEntries) have been
 * moved to dedicated test classes in tests/Cache/Redis/Operations/IntersectionTags/.
 *
 * This file tests the TagSet-specific API methods: tagId, tagKey, flushTag, resetTag.
 *
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
     * @test
     */
    public function testTagIdsReturnsArrayOfTagIdentifiers(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users', 'posts', 'comments']);

        $tagIds = $tagSet->tagIds();

        $this->assertSame([
            'tag:users:entries',
            'tag:posts:entries',
            'tag:comments:entries',
        ], $tagIds);
    }

    /**
     * @test
     */
    public function testGetNamesReturnsOriginalTagNames(): void
    {
        $tagSet = new IntersectionTagSet($this->store, ['users', 'posts']);

        $this->assertSame(['users', 'posts'], $tagSet->getNames());
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
