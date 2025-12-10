<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests remember() and rememberForever() with tags.
 *
 * These operations work similarly in both tagging modes.
 */
final class TaggedRememberCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Tagged Remember Operations';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Remember with tags
        $value = $ctx->cache->tags([$ctx->prefixed('remember')])->remember(
            $ctx->prefixed('tag:remember'),
            60,
            fn (): string => 'remembered-value'
        );

        $result->assert(
            $value === 'remembered-value' && $ctx->cache->get($ctx->prefixed('tag:remember')) === 'remembered-value',
            'remember() with tags stores and returns value'
        );

        // RememberForever with tags
        $value = $ctx->cache->tags([$ctx->prefixed('remember')])->rememberForever(
            $ctx->prefixed('tag:forever'),
            fn (): string => 'forever-value'
        );

        $result->assert(
            $value === 'forever-value' && $ctx->cache->get($ctx->prefixed('tag:forever')) === 'forever-value',
            'rememberForever() with tags stores and returns value'
        );

        return $result;
    }
}
