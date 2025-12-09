<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\IntersectionTags;

use Carbon\Carbon;
use Hypervel\Cache\Redis\Operations\IntersectionTags\AddEntry;
use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;

/**
 * Tests for the AddEntry operation.
 *
 * @internal
 * @coversNothing
 */
class AddEntryTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testAddEntryWithTtl(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 300, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 300, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithZeroTtlStoresNegativeOne(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithNegativeTtlStoresNegativeOne(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', -5, ['tag:users:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenNxCondition(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', 'NX', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries'], 'NX');
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenXxCondition(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', 'XX', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries'], 'XX');
    }

    /**
     * @test
     */
    public function testAddEntryWithUpdateWhenGtCondition(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', 'GT', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, ['tag:users:entries'], 'GT');
    }

    /**
     * @test
     */
    public function testAddEntryWithMultipleTags(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $connection = $this->mockConnection();
        $pipeline = m::mock();

        // Should use pipeline for multiple tags
        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:users:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();
        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('prefix:tag:posts:entries', now()->timestamp + 60, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1, 1]);

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, ['tag:users:entries', 'tag:posts:entries']);
    }

    /**
     * @test
     */
    public function testAddEntryWithEmptyTagsArrayDoesNothing(): void
    {
        $connection = $this->mockConnection();
        // No pipeline or zadd calls should be made
        $connection->shouldNotReceive('multi');
        $connection->shouldNotReceive('zadd');

        $store = $this->createStore($connection);
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 60, []);
    }

    /**
     * @test
     */
    public function testAddEntryUsesCorrectPrefix(): void
    {
        $connection = $this->mockConnection();
        $pipeline = m::mock();

        $connection->shouldReceive('multi')
            ->once()
            ->with(Redis::PIPELINE)
            ->andReturn($pipeline);

        $pipeline->shouldReceive('zadd')
            ->once()
            ->with('custom_prefix:tag:users:entries', -1, 'mykey')
            ->andReturnSelf();

        $pipeline->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection, 'custom_prefix');
        $operation = new AddEntry($store->getContext());

        $operation->execute('mykey', 0, ['tag:users:entries']);
    }
}
