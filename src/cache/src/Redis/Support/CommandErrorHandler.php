<?php

declare(strict_types=1);

namespace Hypervel\Cache\Redis\Support;

use Hypervel\Cache\Exceptions\RedisCacheException;
use Throwable;

/**
 * Handles Redis command errors and provides appropriate exceptions.
 *
 * This class provides consistent error handling across all Redis cache operations,
 * detecting specific error conditions and throwing appropriate exceptions with
 * helpful messages.
 */
class CommandErrorHandler
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
