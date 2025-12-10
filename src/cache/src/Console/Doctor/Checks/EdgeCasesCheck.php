<?php

declare(strict_types=1);

namespace Hypervel\Cache\Console\Doctor\Checks;

use Hypervel\Cache\Console\Doctor\CheckResult;
use Hypervel\Cache\Console\Doctor\DoctorContext;

/**
 * Tests edge cases: null, zero, empty string, special characters, complex data.
 *
 * Most tests are mode-agnostic, but tag hash verification is union mode specific.
 */
final class EdgeCasesCheck implements CheckInterface
{
    public function name(): string
    {
        return 'Edge Cases';
    }

    public function run(DoctorContext $ctx): CheckResult
    {
        $result = new CheckResult();

        // Null values
        $ctx->cache->put($ctx->prefixed('edge:null'), null, 60);
        $result->assert(
            $ctx->cache->has($ctx->prefixed('edge:null')) === false,
            'null values are not stored (Laravel behavior)'
        );

        // Zero values
        $ctx->cache->put($ctx->prefixed('edge:zero'), 0, 60);
        $result->assert(
            (int) $ctx->cache->get($ctx->prefixed('edge:zero')) === 0,
            'Zero values are stored and retrieved'
        );

        // Empty string
        $ctx->cache->put($ctx->prefixed('edge:empty'), '', 60);
        $result->assert(
            $ctx->cache->get($ctx->prefixed('edge:empty')) === '',
            'Empty strings are stored'
        );

        // Numeric tags
        $ctx->cache->tags([$ctx->prefixed('123'), $ctx->prefixed('string-tag')])->put($ctx->prefixed('edge:numeric-tags'), 'value', 60);

        if ($ctx->isUnionMode()) {
            $result->assert(
                $ctx->redis->hexists($ctx->tagHashKey($ctx->prefixed('123')), $ctx->prefixed('edge:numeric-tags')) === true,
                'Numeric tags are handled (cast to strings, union mode)'
            );
        } else {
            // For intersection mode, verify the key was stored
            $result->assert(
                $ctx->cache->get($ctx->prefixed('edge:numeric-tags')) === 'value',
                'Numeric tags are handled (cast to strings, intersection mode)'
            );
        }

        // Special characters in keys
        $ctx->cache->put($ctx->prefixed('edge:special!@#$%'), 'special', 60);
        $result->assert(
            $ctx->cache->get($ctx->prefixed('edge:special!@#$%')) === 'special',
            'Special characters in keys are handled'
        );

        // Complex data structures
        $complex = [
            'nested' => [
                'array' => [1, 2, 3],
                'object' => (object) ['key' => 'value'],
            ],
            'boolean' => true,
            'float' => 3.14159,
        ];
        $ctx->cache->tags([$ctx->prefixed('complex')])->put($ctx->prefixed('edge:complex'), $complex, 60);
        $retrieved = $ctx->cache->get($ctx->prefixed('edge:complex'));
        $result->assert(
            is_array($retrieved) && $retrieved['nested']['array'][0] === 1,
            'Complex data structures are serialized and deserialized'
        );

        return $result;
    }
}
