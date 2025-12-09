<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations;

use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;
use Mockery as m;

/**
 * Tests for the Add operation.
 *
 * @internal
 * @coversNothing
 */
class AddTest extends TestCase
{
    use MocksRedisConnections;

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
}
