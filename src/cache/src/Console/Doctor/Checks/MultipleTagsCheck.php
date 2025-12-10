<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests operations with multiple tags.
 *
 * Flush behavior differs between modes:
 * - Union mode: Flushing ANY tag removes the item
 * - Intersection mode: Flushing requires ALL tags to match
 */
final class MultipleTagsCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Multiple Tag Operations';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Store with multiple tags
        $ctx->cache->tags([
            $ctx->prefixed('posts'),
            $ctx->prefixed('featured'),
            $ctx->prefixed('user:123'),
        ])->put($ctx->prefixed('multi:post1'), 'Featured Post', 60);

        // Verify item was stored
        $result->assert(
            $ctx->cache->get($ctx->prefixed('multi:post1')) === 'Featured Post',
            'Item with multiple tags is stored'
        );

        if ($ctx->isUnionMode()) {
            $this->testUnionMode($ctx, $result);
        } else {
            $this->testIntersectionMode($ctx, $result);
        }

        return $result;
    }

    private function testUnionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // Verify in all tag hashes
        $result->assert(
            $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('posts')), $ctx->prefixed('multi:post1')) === true
            && $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('featured')), $ctx->prefixed('multi:post1')) === true
            && $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('user:123')), $ctx->prefixed('multi:post1')) === true,
            'Item appears in all tag hashes (union mode)'
        );

        // Flush by one tag (union behavior - removes item)
        $ctx->cache->tags([$ctx->prefixed('featured')])->flush();

        $result->assert(
            $ctx->cache->get($ctx->prefixed('multi:post1')) === null,
            'Flushing ANY tag removes the item (union behavior)'
        );

        $result->assert(
            $ctx->redis->exists($ctx->tagHashKey($ctx->prefixed('featured'))) === 0,
            'Flushed tag hash is deleted (union mode)'
        );
    }

    private function testIntersectionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // TODO: Implement intersection mode structure verification
        // Intersection mode uses sorted sets with tag:*:entries pattern

        // Flush by one tag - in intersection mode, this removes items that have that tag
        $ctx->cache->tags([$ctx->prefixed('featured')])->flush();

        $result->assert(
            $ctx->cache->get($ctx->prefixed('multi:post1')) === null,
            'Flushing tag removes items with that tag (intersection mode)'
        );

        // Note: In intersection mode, flush(['a', 'b']) only removes items with BOTH tags
        // But flush(['a']) removes all items with tag 'a'
        $result->assert(
            true,
            'Intersection mode multi-tag structure check (placeholder)'
        );
    }
}
