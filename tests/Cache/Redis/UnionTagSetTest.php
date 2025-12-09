<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Generator;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Cache\Redis\UnionTagSet;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Mockery\MockInterface;

/**
 * @internal
 * @coversNothing
 */
class UnionTagSetTest extends TestCase
{
    private MockInterface|RedisStore $store;

    private MockInterface|StoreContext $context;

    private MockInterface|RedisConnection $connection;

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
    public function testGetNamesReturnsTagNames(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        $this->assertSame(['users', 'posts'], $tagSet->getNames());
    }

    /**
     * @test
     */
    public function testGetNamesReturnsEmptyArrayWhenNoTags(): void
    {
        $tagSet = new UnionTagSet($this->store, []);

        $this->assertSame([], $tagSet->getNames());
    }

    /**
     * @test
     */
    public function testTagIdReturnsTagNameDirectly(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        // Unlike IntersectionTagSet, union mode uses tag name directly (no UUID)
        $this->assertSame('users', $tagSet->tagId('users'));
        $this->assertSame('posts', $tagSet->tagId('posts'));
    }

    /**
     * @test
     */
    public function testTagIdsReturnsAllTagNames(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts', 'comments']);

        $this->assertSame(['users', 'posts', 'comments'], $tagSet->tagIds());
    }

    /**
     * @test
     */
    public function testTagHashKeyDelegatesToContext(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $this->context->shouldReceive('tagHashKey')
            ->once()
            ->with('users')
            ->andReturn('prefix:_erc:tag:users:entries');

        $result = $tagSet->tagHashKey('users');

        $this->assertSame('prefix:_erc:tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testEntriesReturnsGeneratorOfKeys(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $this->store->shouldReceive('getTaggedKeys')
            ->once()
            ->with('users')
            ->andReturn($this->createGenerator(['key1', 'key2', 'key3']));

        $entries = $tagSet->entries();

        $this->assertInstanceOf(Generator::class, $entries);
        $this->assertSame(['key1', 'key2', 'key3'], iterator_to_array($entries));
    }

    /**
     * @test
     */
    public function testEntriesDeduplicatesAcrossTags(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        // 'key2' appears in both tags
        $this->store->shouldReceive('getTaggedKeys')
            ->once()
            ->with('users')
            ->andReturn($this->createGenerator(['key1', 'key2']));

        $this->store->shouldReceive('getTaggedKeys')
            ->once()
            ->with('posts')
            ->andReturn($this->createGenerator(['key2', 'key3']));

        $entries = $tagSet->entries();

        // Should deduplicate 'key2'
        $result = iterator_to_array($entries);
        $this->assertCount(3, $result);
        $this->assertSame(['key1', 'key2', 'key3'], array_values($result));
    }

    /**
     * @test
     */
    public function testEntriesWithEmptyTagReturnsEmpty(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $this->store->shouldReceive('getTaggedKeys')
            ->once()
            ->with('users')
            ->andReturn($this->createGenerator([]));

        $entries = $tagSet->entries();

        $this->assertSame([], iterator_to_array($entries));
    }

    /**
     * @test
     */
    public function testEntriesWithNoTagsReturnsEmpty(): void
    {
        $tagSet = new UnionTagSet($this->store, []);

        $entries = $tagSet->entries();

        $this->assertSame([], iterator_to_array($entries));
    }

    /**
     * @test
     */
    public function testResetCallsFlush(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        $this->store->shouldReceive('flushTags')
            ->once()
            ->with(['users', 'posts']);

        $tagSet->reset();
    }

    /**
     * @test
     */
    public function testFlushDelegatesToStoreFlushTags(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        $this->store->shouldReceive('flushTags')
            ->once()
            ->with(['users', 'posts']);

        $tagSet->flush();
    }

    /**
     * @test
     */
    public function testFlushTagDelegatesToStoreFlushTagsWithSingleTag(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        $this->context->shouldReceive('tagHashKey')
            ->once()
            ->with('users')
            ->andReturn('prefix:_erc:tag:users:entries');

        $this->store->shouldReceive('flushTags')
            ->once()
            ->with(['users']);

        $result = $tagSet->flushTag('users');

        $this->assertSame('prefix:_erc:tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testGetNamespaceReturnsEmptyString(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        // Union mode doesn't namespace keys by tags
        $this->assertSame('', $tagSet->getNamespace());
    }

    /**
     * @test
     */
    public function testResetTagFlushesTagAndReturnsName(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $this->context->shouldReceive('tagHashKey')
            ->once()
            ->with('users')
            ->andReturn('prefix:_erc:tag:users:entries');

        $this->store->shouldReceive('flushTags')
            ->once()
            ->with(['users']);

        $result = $tagSet->resetTag('users');

        // Returns the tag name (not a UUID like IntersectionTagSet)
        $this->assertSame('users', $result);
    }

    /**
     * @test
     */
    public function testTagKeyReturnsSameAsTagHashKey(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $this->context->shouldReceive('tagHashKey')
            ->once()
            ->with('users')
            ->andReturn('prefix:_erc:tag:users:entries');

        $result = $tagSet->tagKey('users');

        $this->assertSame('prefix:_erc:tag:users:entries', $result);
    }

    /**
     * Create a generator from an array for testing.
     */
    private function createGenerator(array $items): Generator
    {
        foreach ($items as $item) {
            yield $item;
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
    }
}
