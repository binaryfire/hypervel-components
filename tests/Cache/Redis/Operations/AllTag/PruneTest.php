<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Operations\AllTag;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hyperf\Redis\RedisFactory;
use Hypervel\Cache\Redis\Operations\AllTag\Prune;
use Hypervel\Cache\RedisStore;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\Cache\Redis\Concerns\MocksRedisConnections;
use Hypervel\Tests\Cache\Redis\Stub\FakeRedisClient;
use Hypervel\Tests\TestCase;
use Mockery as m;

/**
 * Tests for the AllTag/Prune operation.
 *
 * @internal
 * @coversNothing
 */
class PruneTest extends TestCase
{
    use MocksRedisConnections;

    /**
     * @test
     */
    public function testPruneReturnsEmptyStatsWhenNoTagsFound(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN returns no keys
        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator) {
                $iterator = 0;
                return [];
            });

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $result = $operation->execute();

        $this->assertSame(0, $result['tags_scanned']);
        $this->assertSame(0, $result['entries_removed']);
        $this->assertSame(0, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneRemovesStaleEntriesFromSingleTag(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN returns one tag key
        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator, $pattern, $count) {
                $iterator = 0;
                return ['prefix:_all:tag:users:entries'];
            });

        // Pipeline for ZREMRANGEBYSCORE batch (2 pipeline calls: zRemRangeByScore + zCard)
        $client->shouldReceive('pipeline')->twice()->andReturn($client);

        // ZREMRANGEBYSCORE removes 5 stale entries
        $client->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:_all:tag:users:entries', '0', m::type('string'))
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([5]);

        // ZCARD check - returns 3 remaining entries (not empty, so no 3rd pipeline for DEL)
        $client->shouldReceive('zCard')
            ->once()
            ->with('prefix:_all:tag:users:entries')
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([3]);

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $result = $operation->execute();

        $this->assertSame(1, $result['tags_scanned']);
        $this->assertSame(5, $result['entries_removed']);
        $this->assertSame(0, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneDeletesEmptySortedSets(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN returns one tag key
        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator, $pattern, $count) {
                $iterator = 0;
                return ['prefix:_all:tag:users:entries'];
            });

        // Pipeline for ZREMRANGEBYSCORE
        $client->shouldReceive('pipeline')->times(3)->andReturn($client);

        // ZREMRANGEBYSCORE removes all entries
        $client->shouldReceive('zRemRangeByScore')
            ->once()
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([10]);

        // ZCARD check - returns 0 (empty)
        $client->shouldReceive('zCard')
            ->once()
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([0]);

        // DEL for empty set
        $client->shouldReceive('del')
            ->once()
            ->with('prefix:_all:tag:users:entries')
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $result = $operation->execute();

        $this->assertSame(1, $result['tags_scanned']);
        $this->assertSame(10, $result['entries_removed']);
        $this->assertSame(1, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneHandlesMultipleTags(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN returns multiple tag keys
        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator, $pattern, $count) {
                $iterator = 0;
                return [
                    'prefix:_all:tag:users:entries',
                    'prefix:_all:tag:posts:entries',
                    'prefix:_all:tag:comments:entries',
                ];
            });

        // Pipeline calls
        $client->shouldReceive('pipeline')->times(3)->andReturn($client);

        // ZREMRANGEBYSCORE for all tags
        $client->shouldReceive('zRemRangeByScore')
            ->times(3)
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([2, 3, 0]); // Removed entries per tag

        // ZCARD for all tags
        $client->shouldReceive('zCard')
            ->times(3)
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([5, 0, 10]); // Remaining entries per tag (posts is empty)

        // DEL for empty posts tag
        $client->shouldReceive('del')
            ->once()
            ->andReturn($client);
        $client->shouldReceive('exec')
            ->once()
            ->andReturn([1]);

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $result = $operation->execute();

        $this->assertSame(3, $result['tags_scanned']);
        $this->assertSame(5, $result['entries_removed']); // 2 + 3 + 0
        $this->assertSame(1, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneDeduplicatesScanResults(): void
    {
        // Use FakeRedisClient stub for proper reference parameter handling
        // (Mockery's andReturnUsing doesn't propagate &$iterator modifications)
        $fakeClient = new FakeRedisClient(
            scanResults: [
                // First scan: returns 2 keys, iterator = 100 (continue)
                ['keys' => ['prefix:_all:tag:users:entries', 'prefix:_all:tag:posts:entries'], 'iterator' => 100],
                // Second scan: returns 1 duplicate + 1 new, iterator = 0 (done)
                ['keys' => ['prefix:_all:tag:users:entries', 'prefix:_all:tag:comments:entries'], 'iterator' => 0],
            ],
            execResults: [
                [1, 1, 1], // ZREMRANGEBYSCORE results: 1 entry removed per tag
                [5, 5, 5], // ZCARD results: 5 entries remain per tag (none empty)
            ]
        );

        $connection = m::mock(RedisConnection::class);
        $connection->shouldReceive('release')->zeroOrMoreTimes();
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($fakeClient);

        $pool = m::mock(RedisPool::class);
        $pool->shouldReceive('get')->andReturn($connection);

        $poolFactory = m::mock(PoolFactory::class);
        $poolFactory->shouldReceive('getPool')->with('default')->andReturn($pool);

        $store = new RedisStore(
            m::mock(RedisFactory::class),
            'prefix:',
            'default',
            $poolFactory
        );

        $operation = new Prune($store->getContext());
        $result = $operation->execute();

        // Verify scan was called twice (multi-iteration)
        $this->assertSame(2, $fakeClient->getScanCallCount());

        // Verify deduplication: 3 unique tags from 4 total keys scanned
        $this->assertSame(3, $result['tags_scanned']);
        $this->assertSame(3, $result['entries_removed']); // 1 + 1 + 1
        $this->assertSame(0, $result['empty_sets_deleted']); // None empty
    }

    /**
     * @test
     */
    public function testPruneUsesCorrectPrefix(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN should use custom prefix pattern
        $client->shouldReceive('scan')
            ->once()
            ->with(m::any(), 'custom_prefix:_all:tag:*:entries', m::any())
            ->andReturnUsing(function (&$iterator) {
                $iterator = 0;
                return [];
            });

        $store = $this->createStore($connection, 'custom_prefix:');
        $operation = new Prune($store->getContext());

        $operation->execute();
    }

    /**
     * @test
     */
    public function testPruneClusterModeUsesSequentialCommands(): void
    {
        [$store, $clusterClient] = $this->createClusterStore();

        // Cluster mode: _masters() returns array of master nodes
        $masterNode = ['127.0.0.1', 7000];
        $clusterClient->shouldReceive('_masters')
            ->once()
            ->andReturn([$masterNode]);

        // Cluster SCAN signature: scan(&$iterator, $node, $pattern, $count)
        $clusterClient->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator, $node, $pattern, $count) use ($masterNode) {
                $this->assertSame($masterNode, $node);
                $iterator = 0;
                return ['prefix:_all:tag:users:entries'];
            });

        // Should NOT use pipeline in cluster mode
        $clusterClient->shouldNotReceive('pipeline');

        // Sequential commands
        $clusterClient->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:_all:tag:users:entries', '0', m::type('string'))
            ->andReturn(5);

        $clusterClient->shouldReceive('zCard')
            ->once()
            ->with('prefix:_all:tag:users:entries')
            ->andReturn(3);

        // Not empty, so no DEL

        $operation = new Prune($store->getContext());
        $result = $operation->execute();

        $this->assertSame(1, $result['tags_scanned']);
        $this->assertSame(5, $result['entries_removed']);
        $this->assertSame(0, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneClusterModeDeletesEmptySets(): void
    {
        [$store, $clusterClient] = $this->createClusterStore();

        // Cluster mode: _masters() returns array of master nodes
        $masterNode = ['127.0.0.1', 7000];
        $clusterClient->shouldReceive('_masters')
            ->once()
            ->andReturn([$masterNode]);

        // Cluster SCAN signature: scan(&$iterator, $node, $pattern, $count)
        $clusterClient->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator, $node, $pattern, $count) {
                $iterator = 0;
                return ['prefix:_all:tag:users:entries'];
            });

        $clusterClient->shouldReceive('zRemRangeByScore')
            ->once()
            ->andReturn(10);

        $clusterClient->shouldReceive('zCard')
            ->once()
            ->andReturn(0); // Empty after removal

        $clusterClient->shouldReceive('del')
            ->once()
            ->with('prefix:_all:tag:users:entries')
            ->andReturn(1);

        $operation = new Prune($store->getContext());
        $result = $operation->execute();

        $this->assertSame(1, $result['tags_scanned']);
        $this->assertSame(10, $result['entries_removed']);
        $this->assertSame(1, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPrunePreservesForeverItems(): void
    {
        // This is a documentation/behavior test - forever items have score -1
        // ZREMRANGEBYSCORE with lower bound '0' excludes negative scores
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator) {
                $iterator = 0;
                return ['prefix:_all:tag:users:entries'];
            });

        // 2 pipelines: ZREMRANGEBYSCORE + ZCARD (no DEL since not empty)
        $client->shouldReceive('pipeline')->twice()->andReturn($client);

        // Verify lower bound is '0', not '-inf'
        $client->shouldReceive('zRemRangeByScore')
            ->once()
            ->with('prefix:_all:tag:users:entries', '0', m::type('string'))
            ->andReturnUsing(function ($key, $min, $max) use ($client) {
                // Lower bound is '0', so -1 forever items are excluded
                $this->assertSame('0', $min);
                return $client;
            });

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([0]); // 0 entries removed (forever items preserved)

        $client->shouldReceive('zCard')->once()->andReturn($client);

        $client->shouldReceive('exec')
            ->once()
            ->andReturn([5]); // 5 entries remain (not empty, no DEL)

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $operation->execute();
    }

    /**
     * @test
     */
    public function testPruneUsesCustomScanCount(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        // SCAN should use custom count
        $client->shouldReceive('scan')
            ->once()
            ->with(m::any(), m::any(), 500)
            ->andReturnUsing(function (&$iterator) {
                $iterator = 0;
                return [];
            });

        $store = $this->createStore($connection);
        $operation = new Prune($store->getContext());

        $operation->execute(500);
    }

    /**
     * @test
     */
    public function testPruneViaStoreOperationsContainer(): void
    {
        $connection = $this->mockConnection();
        $client = $connection->_mockClient;

        $client->shouldReceive('scan')
            ->once()
            ->andReturnUsing(function (&$iterator) {
                $iterator = 0;
                return [];
            });

        $store = $this->createStore($connection);

        // Access via the operations container
        $result = $store->allTagOps()->prune()->execute();

        $this->assertSame(0, $result['tags_scanned']);
    }

    /**
     * @test
     */
    public function testPruneClusterModeScansAllMasterNodes(): void
    {
        [$store, $clusterClient] = $this->createClusterStore();

        // Cluster with 3 master nodes
        $masterNodes = [
            ['127.0.0.1', 7000],
            ['127.0.0.1', 7001],
            ['127.0.0.1', 7002],
        ];
        $clusterClient->shouldReceive('_masters')
            ->once()
            ->andReturn($masterNodes);

        // Each master returns different tags (simulating distributed keys)
        $scannedNodes = [];
        $clusterClient->shouldReceive('scan')
            ->times(3)
            ->andReturnUsing(function (&$iterator, $node, $pattern, $count) use (&$scannedNodes) {
                $scannedNodes[] = $node;
                $iterator = 0;
                // Each node returns one tag key
                return match ($node[1]) {
                    7000 => ['prefix:_all:tag:users:entries'],
                    7001 => ['prefix:_all:tag:posts:entries'],
                    7002 => ['prefix:_all:tag:comments:entries'],
                    default => [],
                };
            });

        // Sequential commands for each tag (cluster mode)
        $clusterClient->shouldReceive('zRemRangeByScore')
            ->times(3)
            ->andReturn(2);

        $clusterClient->shouldReceive('zCard')
            ->times(3)
            ->andReturn(5); // Not empty

        $operation = new Prune($store->getContext());
        $result = $operation->execute();

        // Verify all 3 master nodes were scanned
        $this->assertCount(3, $scannedNodes);
        $this->assertContains(['127.0.0.1', 7000], $scannedNodes);
        $this->assertContains(['127.0.0.1', 7001], $scannedNodes);
        $this->assertContains(['127.0.0.1', 7002], $scannedNodes);

        // Verify stats aggregate across all nodes
        $this->assertSame(3, $result['tags_scanned']);
        $this->assertSame(6, $result['entries_removed']); // 2 + 2 + 2
        $this->assertSame(0, $result['empty_sets_deleted']);
    }

    /**
     * @test
     */
    public function testPruneClusterModeDeduplicatesAcrossNodes(): void
    {
        [$store, $clusterClient] = $this->createClusterStore();

        // Two master nodes
        $masterNodes = [
            ['127.0.0.1', 7000],
            ['127.0.0.1', 7001],
        ];
        $clusterClient->shouldReceive('_masters')
            ->once()
            ->andReturn($masterNodes);

        // Both nodes return the same tag (edge case - shouldn't happen often but should be handled)
        $clusterClient->shouldReceive('scan')
            ->times(2)
            ->andReturnUsing(function (&$iterator, $node, $pattern, $count) {
                $iterator = 0;
                // Both return same tag (simulating possible inconsistency during rebalancing)
                return ['prefix:_all:tag:users:entries'];
            });

        // Should only process the tag ONCE (deduplicated)
        $clusterClient->shouldReceive('zRemRangeByScore')
            ->once()
            ->andReturn(5);

        $clusterClient->shouldReceive('zCard')
            ->once()
            ->andReturn(3);

        $operation = new Prune($store->getContext());
        $result = $operation->execute();

        $this->assertSame(1, $result['tags_scanned']); // Deduplicated
        $this->assertSame(5, $result['entries_removed']);
    }
}
