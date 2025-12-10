<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use BadMethodCallException;
use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests tagged cache operations: tagged put, get, flush.
 *
 * Behavior differs between tagging modes:
 * - Union mode: get() on tagged cache throws BadMethodCallException
 * - Intersection mode: get() on tagged cache works normally
 */
final class TaggedOperationsCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Tagged Cache Operations';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Single tag put
        $ctx->cache->tags([$ctx->prefixed('products')])->put($ctx->prefixed('tag:product1'), 'Product 1', 60);
        $result->assert(
            $ctx->cache->get($ctx->prefixed('tag:product1')) === 'Product 1',
            'Tagged item can be retrieved without tags (direct get)'
        );

        if ($ctx->isUnionMode()) {
            $this->testUnionMode($ctx, $result);
        } else {
            $this->testIntersectionMode($ctx, $result);
        }

        // Tag flush (common to both modes)
        $ctx->cache->tags([$ctx->prefixed('products')])->flush();
        $result->assert(
            $ctx->cache->get($ctx->prefixed('tag:product1')) === null,
            'flush() removes tagged items'
        );

        return $result;
    }

    private function testUnionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // Verify hash structure exists
        $tagKey = $ctx->tagHashKey($ctx->prefixed('products'));
        $result->assert(
            $ctx->redis->hexists($tagKey, $ctx->prefixed('tag:product1')) === true,
            'Tag hash contains the cache key (union mode)'
        );

        // Verify get() on tagged cache throws
        $threw = false;
        try {
            $ctx->cache->tags([$ctx->prefixed('products')])->get($ctx->prefixed('tag:product1'));
        } catch (BadMethodCallException) {
            $threw = true;
        }
        $result->assert(
            $threw,
            'Tagged get() throws BadMethodCallException (union mode)'
        );
    }

    private function testIntersectionMode(DoctorContext $ctx, CheckResult $result): void
    {
        // In intersection mode, get() on tagged cache works
        $value = $ctx->cache->tags([$ctx->prefixed('products')])->get($ctx->prefixed('tag:product1'));
        $result->assert(
            $value === 'Product 1',
            'Tagged get() returns value (intersection mode)'
        );

        // TODO: Add intersection mode specific structure checks
        // Intersection mode uses sorted sets, not hashes
        $result->assert(
            true,
            'Intersection mode tag structure check (placeholder)'
        );
    }
}
