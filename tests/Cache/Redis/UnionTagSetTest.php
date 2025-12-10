<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Generator;
use Hypervel\Cache\Redis\UnionTagSet;
use Hypervel\Cache\RedisStore;
use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;

/**
 * Tests for UnionTagSet class.
 *
 * Uses MocksRedisConnections to mock at the Redis client level,
 * allowing the full operation chain to execute.
 *
 * @internal
 * @coversNothing
 */
class UnionTagSetTest extends TestCase
{
    use MocksRedisConnections;

    private RedisStore $store;

    private m\MockInterface $client;

    private m\MockInterface $pipeline;

    /**
     * Set up test fixtures.
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->setupStore();
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
    public function testTagHashKeyReturnsCorrectFormat(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        $result = $tagSet->tagHashKey('users');

        $this->assertSame('prefix:_erc:tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testEntriesReturnsGeneratorOfKeys(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        // GetTaggedKeys checks HLEN then uses HKEYS for small hashes
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(3);

        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1', 'key2', 'key3']);

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

        // First tag 'users' has keys key1, key2
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(2);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1', 'key2']);

        // Second tag 'posts' has keys key2, key3 (key2 is duplicate)
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:posts:entries')
            ->andReturn(2);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:posts:entries')
            ->andReturn(['key2', 'key3']);

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

        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(0);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn([]);

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
    public function testFlushDeletesKeysAndTagHashes(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users']);

        // GetTaggedKeys for the flush operation
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(2);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1', 'key2']);

        // Pipeline for deleting cache keys, reverse indexes, tag hashes, registry entries
        $this->client->shouldReceive('pipeline')->andReturn($this->pipeline);
        $this->pipeline->shouldReceive('del')->andReturnSelf();
        $this->pipeline->shouldReceive('unlink')->andReturnSelf();
        $this->pipeline->shouldReceive('zrem')->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->andReturn([]);

        $tagSet->flush();

        // If we get here without exception, the flush executed through the full chain
        $this->assertTrue(true);
    }

    /**
     * @test
     */
    public function testFlushTagDeletesSingleTag(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        // GetTaggedKeys for the flush operation (only 'users' tag)
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(1);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1']);

        // Pipeline for flush operations
        $this->client->shouldReceive('pipeline')->andReturn($this->pipeline);
        $this->pipeline->shouldReceive('del')->andReturnSelf();
        $this->pipeline->shouldReceive('unlink')->andReturnSelf();
        $this->pipeline->shouldReceive('zrem')->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->andReturn([]);

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

        // GetTaggedKeys for the flush operation
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(1);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1']);

        // Pipeline for flush operations
        $this->client->shouldReceive('pipeline')->andReturn($this->pipeline);
        $this->pipeline->shouldReceive('del')->andReturnSelf();
        $this->pipeline->shouldReceive('unlink')->andReturnSelf();
        $this->pipeline->shouldReceive('zrem')->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->andReturn([]);

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

        $result = $tagSet->tagKey('users');

        $this->assertSame('prefix:_erc:tag:users:entries', $result);
    }

    /**
     * @test
     */
    public function testResetCallsFlush(): void
    {
        $tagSet = new UnionTagSet($this->store, ['users', 'posts']);

        // GetTaggedKeys for both tags
        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(1);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:users:entries')
            ->andReturn(['key1']);

        $this->client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_erc:tag:posts:entries')
            ->andReturn(1);
        $this->client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_erc:tag:posts:entries')
            ->andReturn(['key2']);

        // Pipeline for flush operations
        $this->client->shouldReceive('pipeline')->andReturn($this->pipeline);
        $this->pipeline->shouldReceive('del')->andReturnSelf();
        $this->pipeline->shouldReceive('unlink')->andReturnSelf();
        $this->pipeline->shouldReceive('zrem')->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->andReturn([]);

        $tagSet->reset();

        // If we get here without exception, reset executed flush correctly
        $this->assertTrue(true);
    }

    /**
     * Set up the store with mocked Redis connection.
     */
    private function setupStore(): void
    {
        $connection = $this->mockConnection();
        $this->client = $connection->_mockClient;

        // Mock pipeline
        $this->pipeline = m::mock();

        // Add pipeline support to client
        $this->client->shouldReceive('pipeline')->andReturn($this->pipeline)->byDefault();

        $this->store = $this->createStore($connection);
    }
}
