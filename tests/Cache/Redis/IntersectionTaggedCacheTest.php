<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis;

use Carbon\Carbon;
use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
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
class IntersectionTaggedCacheTest extends TestCase
{
    private RedisStore $redis;

    /**
     * Mock for RedisConnection (all operations go through PoolFactory now).
     */
    private MockInterface|RedisConnection $connection;

    /**
     * Mock for pipeline operations.
     */
    private MockInterface $pipeline;

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
    public function testTagEntriesCanBeStoredForever(): void
    {
        $key = sha1('tag:people:entries|tag:author:entries') . ':name';

        // AddEntry uses pipeline for zadd operations
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', -1, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', -1, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);

        // Cache operation (set) via RedisConnection
        $this->connection->shouldReceive('set')->once()->with("prefix:{$key}", serialize('Sally'))->andReturn(true);

        $this->redis->tags(['people', 'author'])->forever('name', 'Sally');

        $key = sha1('tag:people:entries|tag:author:entries') . ':age';
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', -1, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', -1, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);
        $this->connection->shouldReceive('set')->once()->with("prefix:{$key}", 30)->andReturn(true);

        $this->redis->tags(['people', 'author'])->forever('age', 30);

        // Flush: entries() scans via RedisConnection
        $this->connection
            ->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:people:entries', null, '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return ['tag:people:entries:name' => 0, 'tag:people:entries:age' => 0];
            });
        $this->connection
            ->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:people:entries', 0, '*', 1000)
            ->andReturnNull();
        $this->connection
            ->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:author:entries', null, '*', 1000)
            ->andReturnUsing(function ($key, &$cursor) {
                $cursor = 0;

                return ['tag:author:entries:name' => 0, 'tag:author:entries:age' => 0];
            });
        $this->connection
            ->shouldReceive('zScan')
            ->once()
            ->with('prefix:tag:author:entries', 0, '*', 1000)
            ->andReturnNull();

        // flushValues() deletes cache entries via RedisConnection
        $this->connection->shouldReceive('del')->once()->with(
            'prefix:tag:people:entries:name',
            'prefix:tag:people:entries:age',
            'prefix:tag:author:entries:name',
            'prefix:tag:author:entries:age'
        )->andReturn(4);

        // flushTags() deletes tag sets in a single batched call
        $this->connection->shouldReceive('del')->once()->with(
            'prefix:tag:people:entries',
            'prefix:tag:author:entries'
        )->andReturn(2);

        $this->redis->tags(['people', 'author'])->flush();
    }

    /**
     * @test
     */
    public function testTagEntriesCanBeIncremented(): void
    {
        $key = sha1('tag:votes:entries') . ':person-1';

        // AddEntry uses pipeline for zadd operations (4 times - 2 increments + 2 decrements)
        // Options must be array format for phpredis
        $this->pipeline->shouldReceive('zadd')->times(4)->with('prefix:tag:votes:entries', ['NX'], -1, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->times(4)->andReturn([1]);

        // Cache operations (increment/decrement) via RedisConnection
        $this->connection->shouldReceive('incrby')->once()->with("prefix:{$key}", 1)->andReturn(1);
        $this->connection->shouldReceive('incrby')->once()->with("prefix:{$key}", 1)->andReturn(2);
        $this->connection->shouldReceive('decrby')->once()->with("prefix:{$key}", 1)->andReturn(1);
        $this->connection->shouldReceive('decrby')->once()->with("prefix:{$key}", 1)->andReturn(0);

        $this->assertSame(1, $this->redis->tags(['votes'])->increment('person-1'));
        $this->assertSame(2, $this->redis->tags(['votes'])->increment('person-1'));

        $this->assertSame(1, $this->redis->tags(['votes'])->decrement('person-1'));
        $this->assertSame(0, $this->redis->tags(['votes'])->decrement('person-1'));
    }

    /**
     * @test
     */
    public function testStaleEntriesCanBeFlushed(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        // FlushStaleEntries uses pipeline for zRemRangeByScore
        $this->pipeline->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:tag:people:entries', '0', (string) now()->timestamp)
            ->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([0]);

        $this->redis->tags(['people'])->flushStale();
    }

    /**
     * @test
     */
    public function testPut(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $key = sha1('tag:people:entries|tag:author:entries') . ':name';

        // AddEntry uses pipeline for zadd operations
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);

        // Cache operation (setex) via RedisConnection
        $this->connection->shouldReceive('setex')->once()->with("prefix:{$key}", 5, serialize('Sally'))->andReturn(true);

        $this->redis->tags(['people', 'author'])->put('name', 'Sally', 5);

        $key = sha1('tag:people:entries|tag:author:entries') . ':age';
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);
        $this->connection->shouldReceive('setex')->once()->with("prefix:{$key}", 5, 30)->andReturn(true);

        $this->redis->tags(['people', 'author'])->put('age', 30, 5);
    }

    /**
     * @test
     */
    public function testPutWithArray(): void
    {
        Carbon::setTestNow('2000-01-01 00:00:00');

        $key = sha1('tag:people:entries|tag:author:entries') . ':name';

        // AddEntry uses pipeline for zadd operations
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);

        // Cache operation (setex) via RedisConnection
        $this->connection->shouldReceive('setex')->once()->with("prefix:{$key}", 5, serialize('Sally'))->andReturn(true);

        $key = sha1('tag:people:entries|tag:author:entries') . ':age';
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:people:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('zadd')->once()->with('prefix:tag:author:entries', now()->timestamp + 5, $key)->andReturnSelf();
        $this->pipeline->shouldReceive('exec')->once()->andReturn([1, 1]);
        $this->connection->shouldReceive('setex')->once()->with("prefix:{$key}", 5, 30)->andReturn(true);

        $this->redis->tags(['people', 'author'])->put([
            'name' => 'Sally',
            'age' => 30,
        ], 5);
    }

    /**
     * Set up the Redis mocks.
     *
     * All operations now go through PoolFactory -> RedisConnection.
     * No more dual paths (RedisProxy vs RedisConnection).
     */
    private function mockRedis(): void
    {
        // Mock pipeline for batched operations
        $this->pipeline = m::mock();

        // Anonymous mock for Redis client (needed for isCluster() check)
        $client = m::mock();
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE)
            ->byDefault();
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_PREFIX)
            ->andReturn('')
            ->byDefault();

        // Mock RedisConnection for all operations
        $this->connection = m::mock(RedisConnection::class);
        $this->connection->shouldReceive('release')->zeroOrMoreTimes();
        $this->connection->shouldReceive('serialized')->andReturn(false)->byDefault();
        $this->connection->shouldReceive('client')->andReturn($client)->byDefault();
        $this->connection->shouldReceive('multi')
            ->with(Redis::PIPELINE)
            ->andReturn($this->pipeline)
            ->byDefault();

        $pool = m::mock(RedisPool::class);
        $pool->shouldReceive('get')->andReturn($this->connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        // RedisFactory is still needed for RedisStore but not used for operations anymore
        $redisFactory = m::mock(RedisFactory::class);

        $this->redis = new RedisStore($redisFactory, 'prefix', 'default', $poolFactory);
    }
}
