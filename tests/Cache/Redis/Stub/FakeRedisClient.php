<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Stub;

use Redis;

/**
 * Fake Redis client for testing SCAN/HSCAN operations with proper reference parameter handling.
 *
 * Mockery's andReturnUsing doesn't properly propagate modifications to reference
 * parameters back to the caller. This stub properly implements the &$iterator
 * reference parameter behavior that phpredis's scan()/hScan() uses.
 *
 * Usage for SCAN:
 * ```php
 * $client = new FakeRedisClient(
 *     scanResults: [
 *         ['keys' => ['key1', 'key2'], 'iterator' => 100],  // First scan: continue
 *         ['keys' => ['key3'], 'iterator' => 0],            // Second scan: done
 *     ]
 * );
 * ```
 *
 * Usage for HSCAN:
 * ```php
 * $client = new FakeRedisClient(
 *     hScanResults: [
 *         'hash:key' => [
 *             ['fields' => ['f1' => 'v1', 'f2' => 'v2'], 'iterator' => 100],
 *             ['fields' => ['f3' => 'v3'], 'iterator' => 0],
 *         ],
 *     ]
 * );
 * ```
 */
class FakeRedisClient
{
    /**
     * Configured scan results: array of ['keys' => [...], 'iterator' => int].
     *
     * @var array<int, array{keys: array<string>, iterator: int}>
     */
    private array $scanResults;

    /**
     * Current scan call index.
     */
    private int $scanCallIndex = 0;

    /**
     * Recorded scan calls for assertions.
     *
     * @var array<int, array{pattern: ?string, count: int}>
     */
    private array $scanCalls = [];

    /**
     * Configured hScan results per hash key.
     *
     * @var array<string, array<int, array{fields: array<string, string>, iterator: int}>>
     */
    private array $hScanResults;

    /**
     * Current hScan call index per hash key.
     *
     * @var array<string, int>
     */
    private array $hScanCallIndex = [];

    /**
     * Recorded hScan calls for assertions.
     *
     * @var array<int, array{key: string, pattern: string, count: int}>
     */
    private array $hScanCalls = [];

    /**
     * Pipeline mode flag.
     */
    private bool $inPipeline = false;

    /**
     * Queued pipeline commands.
     *
     * @var array<int, array{method: string, args: array, result: mixed}>
     */
    private array $pipelineQueue = [];

    /**
     * Configured exec() results for pipeline operations.
     *
     * @var array<int, array<mixed>>
     */
    private array $execResults = [];

    /**
     * Current exec call index.
     */
    private int $execCallIndex = 0;

    /**
     * Configured zRange results per key.
     *
     * @var array<string, array<string>>
     */
    private array $zRangeResults = [];

    /**
     * Configured hLen results per key.
     *
     * @var array<string, int>
     */
    private array $hLenResults = [];

    /**
     * Create a new fake Redis client.
     *
     * @param array<int, array{keys: array<string>, iterator: int}> $scanResults Configured scan results
     * @param array<int, array<mixed>> $execResults Configured exec() results for pipelines
     * @param array<string, array<int, array{fields: array<string, string>, iterator: int}>> $hScanResults Configured hScan results
     * @param array<string, array<string>> $zRangeResults Configured zRange results
     * @param array<string, int> $hLenResults Configured hLen results
     */
    public function __construct(
        array $scanResults = [],
        array $execResults = [],
        array $hScanResults = [],
        array $zRangeResults = [],
        array $hLenResults = [],
    ) {
        $this->scanResults = $scanResults;
        $this->execResults = $execResults;
        $this->hScanResults = $hScanResults;
        $this->zRangeResults = $zRangeResults;
        $this->hLenResults = $hLenResults;
    }

    /**
     * Simulate Redis SCAN with proper reference parameter handling.
     *
     * @param int|string|null $iterator Cursor (modified by reference)
     * @param string|null $pattern Optional pattern to match
     * @param int $count Optional count hint
     * @return array<string>|false
     */
    public function scan(int|string|null &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        // Record the call for assertions
        $this->scanCalls[] = ['pattern' => $pattern, 'count' => $count];

        if (! isset($this->scanResults[$this->scanCallIndex])) {
            $iterator = 0;
            return false;
        }

        $result = $this->scanResults[$this->scanCallIndex];
        $iterator = $result['iterator'];
        $this->scanCallIndex++;

        return $result['keys'];
    }

    /**
     * Get recorded scan calls for test assertions.
     *
     * @return array<int, array{pattern: ?string, count: int}>
     */
    public function getScanCalls(): array
    {
        return $this->scanCalls;
    }

    /**
     * Get the number of scan() calls made.
     */
    public function getScanCallCount(): int
    {
        return count($this->scanCalls);
    }

    /**
     * Simulate Redis HSCAN with proper reference parameter handling.
     *
     * @param string $key Hash key
     * @param int|string|null $iterator Cursor (modified by reference)
     * @param string $pattern Optional pattern to match
     * @param int $count Optional count hint
     * @return array<string, string>|false
     */
    public function hScan(string $key, int|string|null &$iterator, string $pattern = '*', int $count = 0): array|false
    {
        // Record the call for assertions
        $this->hScanCalls[] = ['key' => $key, 'pattern' => $pattern, 'count' => $count];

        // Initialize call index for this key if not set
        if (! isset($this->hScanCallIndex[$key])) {
            $this->hScanCallIndex[$key] = 0;
        }

        if (! isset($this->hScanResults[$key][$this->hScanCallIndex[$key]])) {
            $iterator = 0;
            return false;
        }

        $result = $this->hScanResults[$key][$this->hScanCallIndex[$key]];
        $iterator = $result['iterator'];
        $this->hScanCallIndex[$key]++;

        return $result['fields'];
    }

    /**
     * Get recorded hScan calls for test assertions.
     *
     * @return array<int, array{key: string, pattern: string, count: int}>
     */
    public function getHScanCalls(): array
    {
        return $this->hScanCalls;
    }

    /**
     * Get the number of hScan() calls made.
     */
    public function getHScanCallCount(): int
    {
        return count($this->hScanCalls);
    }

    /**
     * Simulate getOption() for compression and prefix checks.
     */
    public function getOption(int $option): mixed
    {
        return match ($option) {
            Redis::OPT_COMPRESSION => Redis::COMPRESSION_NONE,
            Redis::OPT_PREFIX => '',
            default => null,
        };
    }

    /**
     * Simulate zRange to get sorted set members.
     *
     * @return array<string>
     */
    public function zRange(string $key, int $start, int $end): array
    {
        return $this->zRangeResults[$key] ?? [];
    }

    /**
     * Simulate hLen to get hash length.
     */
    public function hLen(string $key): int
    {
        return $this->hLenResults[$key] ?? 0;
    }

    /**
     * Queue exists in pipeline or execute directly.
     *
     * @return $this|int
     */
    public function exists(string $key): static|int
    {
        if ($this->inPipeline) {
            $this->pipelineQueue[] = ['method' => 'exists', 'args' => [$key]];
            return $this;
        }
        return 0;
    }

    /**
     * Queue hDel in pipeline or execute directly.
     *
     * @return $this|int
     */
    public function hDel(string $key, string ...$fields): static|int
    {
        if ($this->inPipeline) {
            $this->pipelineQueue[] = ['method' => 'hDel', 'args' => [$key, ...$fields]];
            return $this;
        }
        return count($fields);
    }

    /**
     * Enter pipeline mode.
     *
     * @return $this
     */
    public function pipeline(): static
    {
        $this->inPipeline = true;
        $this->pipelineQueue = [];
        return $this;
    }

    /**
     * Execute pipeline and return results.
     *
     * @return array<mixed>
     */
    public function exec(): array
    {
        $this->inPipeline = false;

        if (isset($this->execResults[$this->execCallIndex])) {
            $result = $this->execResults[$this->execCallIndex];
            $this->execCallIndex++;
            return $result;
        }

        // Return empty array if no more configured results
        return [];
    }

    /**
     * Queue zRemRangeByScore in pipeline or execute directly.
     *
     * @return $this|int
     */
    public function zRemRangeByScore(string $key, string $min, string $max): static|int
    {
        if ($this->inPipeline) {
            $this->pipelineQueue[] = ['method' => 'zRemRangeByScore', 'args' => [$key, $min, $max]];
            return $this;
        }
        return 0;
    }

    /**
     * Queue zCard in pipeline or execute directly.
     *
     * @return $this|int
     */
    public function zCard(string $key): static|int
    {
        if ($this->inPipeline) {
            $this->pipelineQueue[] = ['method' => 'zCard', 'args' => [$key]];
            return $this;
        }
        return 0;
    }

    /**
     * Queue del in pipeline or execute directly.
     *
     * @return $this|int
     */
    public function del(string ...$keys): static|int
    {
        if ($this->inPipeline) {
            $this->pipelineQueue[] = ['method' => 'del', 'args' => $keys];
            return $this;
        }
        return count($keys);
    }

    /**
     * Reset the client state for reuse.
     */
    public function reset(): void
    {
        $this->scanCallIndex = 0;
        $this->scanCalls = [];
        $this->hScanCallIndex = [];
        $this->hScanCalls = [];
        $this->execCallIndex = 0;
        $this->inPipeline = false;
        $this->pipelineQueue = [];
    }
}
