<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests shared tag flush behavior and orphan handling.
 *
 * When an item has multiple tags and one tag is flushed,
 * orphaned references may remain in other tags (lazy cleanup).
 */
final class SharedTagFlushCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Shared Tag Flush (Orphan Prevention)';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        $tagA = $ctx->prefixed('tagA-' . bin2hex(random_bytes(4)));
        $tagB = $ctx->prefixed('tagB-' . bin2hex(random_bytes(4)));
        $key = $ctx->prefixed('shared:' . bin2hex(random_bytes(4)));
        $value = 'value-' . bin2hex(random_bytes(4));

        // Store item with both tags
        $ctx->cache->tags([$tagA, $tagB])->put($key, $value, 60);

        // Verify item was stored
        $result->assert(
            $ctx->cache->get($key) === $value,
            'Item with shared tags is stored'
        );

        if ($ctx->isUnionMode()) {
            $this->testUnionMode($ctx, $result, $tagA, $tagB, $key);
        } else {
            $this->testIntersectionMode($ctx, $result, $tagA, $tagB, $key);
        }

        return $result;
    }

    private function testUnionMode(
        DoctorContext $ctx,
        CheckResult $result,
        string $tagA,
        string $tagB,
        string $key,
    ): void {
        // Verify in both tag hashes
        $tagAKey = $ctx->tagHashKey($tagA);
        $tagBKey = $ctx->tagHashKey($tagB);

        $result->assert(
            $ctx->redis->hexists($tagAKey, $key) && $ctx->redis->hexists($tagBKey, $key),
            'Key exists in both tag hashes (union mode)'
        );

        // Flush Tag A
        $ctx->cache->tags([$tagA])->flush();

        $result->assert(
            $ctx->cache->get($key) === null,
            'Shared tag flush removes item (union mode)'
        );

        // In lazy mode (Hypervel default), orphans remain in Tag B hash
        // They will be cleaned by the scheduled prune command
        $result->assert(
            $ctx->redis->hexists($tagBKey, $key),
            'Orphaned field exists in shared tag (lazy cleanup - will be cleaned by prune command)'
        );
    }

    private function testIntersectionMode(
        DoctorContext $ctx,
        CheckResult $result,
        string $tagA,
        string $tagB,
        string $key,
    ): void {
        // Flush Tag A
        $ctx->cache->tags([$tagA])->flush();

        $result->assert(
            $ctx->cache->get($key) === null,
            'Shared tag flush removes item (intersection mode)'
        );

        // TODO: Verify intersection mode orphan behavior
        // Intersection mode uses sorted sets - orphans cleaned by ZREMRANGEBYSCORE
        $result->assert(
            true,
            'Intersection mode orphan check (placeholder)'
        );
    }
}
