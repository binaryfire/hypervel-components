<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\AnyTag;

use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\TestCase;

/**
 * Tests for the GetTaggedKeys operation (union tags).
 *
 * @internal
 * @coversNothing
 */
class GetTaggedKeysTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testGetTaggedKeysUsesHkeysForSmallHashes(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // Small hash (below threshold) uses HKEYS
        $client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_any:tag:users:entries')
            ->andReturn(5);
        $client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_any:tag:users:entries')
            ->andReturn(['key1', 'key2', 'key3']);

        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $keys = iterator_to_array($redis->anyTagOps()->getTaggedKeys()->execute('users'));

        $this->assertSame(['key1', 'key2', 'key3'], $keys);
    }

    /**
     * @test
     */
    public function testGetTaggedKeysUsesHscanForLargeHashes(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // Large hash (above threshold of 1000) uses HSCAN
        $client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_any:tag:users:entries')
            ->andReturn(5000);

        // HSCAN returns key-value pairs, iterator updates by reference
        $client->shouldReceive('hscan')
            ->once()
            ->withArgs(function ($key, &$iterator, $pattern, $count) {
                $iterator = 0; // Done after first iteration
                return true;
            })
            ->andReturn(['key1' => '1', 'key2' => '1', 'key3' => '1']);

        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $keys = iterator_to_array($redis->anyTagOps()->getTaggedKeys()->execute('users'));

        $this->assertSame(['key1', 'key2', 'key3'], $keys);
    }

    /**
     * @test
     */
    public function testGetTaggedKeysReturnsEmptyForNonExistentTag(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('hlen')
            ->once()
            ->with('prefix:_any:tag:nonexistent:entries')
            ->andReturn(0);
        $client->shouldReceive('hkeys')
            ->once()
            ->with('prefix:_any:tag:nonexistent:entries')
            ->andReturn([]);

        $redis = $this->createStore($connection);
        $redis->setTagMode('any');
        $keys = iterator_to_array($redis->anyTagOps()->getTaggedKeys()->execute('nonexistent'));

        $this->assertSame([], $keys);
    }
}
