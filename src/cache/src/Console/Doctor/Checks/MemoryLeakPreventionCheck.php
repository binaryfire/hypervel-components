<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests memory leak prevention through tag reference expiration.
 *
 * Union mode: Hash fields auto-expire via HEXPIRE.
 * Intersection mode: Sorted set entries cleaned via ZREMRANGEBYSCORE.
 *
 * Hypervel only supports lazy cleanup mode (orphans cleaned by scheduled command).
 */
final class MemoryLeakPreventionCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Memory Leak Prevention';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        if ($ctx->isUnionMode()) {
            $this->testUnionMode($ctx, $result);
        } else {
            $this->testIntersectionMode($ctx, $result);
        }

        return $result;
    }

    private function testUnionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // Create item with short TTL
        $ctx->cache->tags([$ctx->prefixed('leak-test')])->put($ctx->prefixed('leak:short'), 'value', 3);

        $tagKey = $ctx->tagHashKey($ctx->prefixed('leak-test'));

        // Verify field has expiration
        $ttl = $ctx->redis->httl($tagKey, [$ctx->prefixed('leak:short')]);
        $result->assert(
            $ttl[0] > 0 && $ttl[0] <= 3,
            'Hash field has TTL set (will auto-expire)'
        );

        // Test lazy cleanup after flush
        $ctx->cache->tags([$ctx->prefixed('alpha'), $ctx->prefixed('beta')])->put($ctx->prefixed('leak:shared'), 'value', 60);

        // Flush one tag
        $ctx->cache->tags([$ctx->prefixed('alpha')])->flush();

        // Alpha hash should be deleted
        $result->assert(
            $ctx->redis->exists($ctx->tagHashKey($ctx->prefixed('alpha'))) === 0,
            'Flushed tag hash is deleted'
        );

        // Hypervel uses lazy cleanup mode - orphans remain until prune command runs
        $result->assert(
            $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('beta')), $ctx->prefixed('leak:shared')),
            'Orphaned field exists in shared tag hash (lazy cleanup - will be cleaned by prune command)'
        );
    }

    private function testIntersectionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // TODO: Implement intersection mode memory leak prevention tests
        // Intersection mode uses sorted sets with TTL as score
        // ZREMRANGEBYSCORE cleans up expired entries
        $result->assert(
            true,
            'Intersection mode memory leak prevention (placeholder)'
        );
    }
}
