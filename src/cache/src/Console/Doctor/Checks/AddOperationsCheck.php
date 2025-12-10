<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests add() operation (only stores if key doesn't exist).
 *
 * This operation is mode-agnostic (works the same in both modes).
 */
final class AddOperationsCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Add Operations (Only If Not Exists)';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Add new key
        $addResult = $ctx->cache->add($ctx->prefixed('add:new'), 'first', 60);
        $result->assert(
            $addResult === true && $ctx->cache->get($ctx->prefixed('add:new')) === 'first',
            'add() succeeds for non-existent key'
        );

        // Try to add existing key
        $addResult = $ctx->cache->add($ctx->prefixed('add:new'), 'second', 60);
        $result->assert(
            $addResult === false && $ctx->cache->get($ctx->prefixed('add:new')) === 'first',
            'add() fails for existing key (value unchanged)'
        );

        // Add with tags
        $addResult = $ctx->cache->tags([$ctx->prefixed('unique')])->add($ctx->prefixed('add:tagged'), 'value', 60);
        $result->assert(
            $addResult === true,
            'add() works with tags'
        );

        $addResult = $ctx->cache->tags([$ctx->prefixed('unique')])->add($ctx->prefixed('add:tagged'), 'new value', 60);
        $result->assert(
            $addResult === false,
            'add() with tags fails for existing key'
        );

        return $result;
    }
}
