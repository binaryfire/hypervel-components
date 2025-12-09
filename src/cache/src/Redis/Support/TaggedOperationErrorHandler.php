<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Support;

use Hypervel\Cache\Exceptions\RedisCacheException;
use Throwable;

/**
 * Handles Redis command errors specific to tagged cache operations.
 *
 * Tagged operations use Redis 8.0+ commands (HSETEX, HEXPIRE) for hash field
 * expiration. This handler detects and provides helpful error messages for:
 *
 * 1. Redis version errors - HSETEX/HEXPIRE require Redis 8.0+ or Valkey 9.0+
 * 2. Cluster cross-slot errors - Bug detection for improperly batched operations
 *
 * This handler is ONLY used in *WithTags operations. Simple operations (get, put,
 * increment, etc.) let exceptions bubble up naturally without special handling.
 */
class TaggedOperationErrorHandler
{
    /**
     * Handle a Redis command exception.
     *
     * @throws RedisCacheException When HSETEX is unavailable (Redis < 8.0) or cluster errors occur
     * @throws Throwable Re-throws the original exception if not a recognized error
     */
    public static function handle(Throwable $e): never
    {
        $message = strtolower($e->getMessage());

        // Check for HSETEX/HEXPIRE unavailability (Redis < 8.0)
        if (str_contains($message, 'unknown command')
            || str_contains($message, 'err unknown')
            || str_contains($message, 'command not found')) {
            throw new RedisCacheException(
                'Failed to execute Redis command. Union tagging requires Redis 8.0+ or Valkey 9.0+ ' .
                'for hash field expiration support (HSETEX, HEXPIRE commands). ' .
                'Also ensure phpredis extension 6.3.0+ is installed. ' .
                'Run `php artisan cache:doctor` to check your environment. ' .
                'Original error: ' . $e->getMessage(),
                previous: $e
            );
        }

        // Check for cluster cross-slot errors
        if (str_contains($message, 'crossslot')) {
            throw new RedisCacheException(
                'Cross-slot operation attempted. This is a bug in the cache driver - ' .
                'cluster mode should use sequential commands to avoid cross-slot errors. ' .
                'Please report this issue. Original error: ' . $e->getMessage(),
                previous: $e
            );
        }

        // Re-throw other exceptions
        throw $e;
    }
}
