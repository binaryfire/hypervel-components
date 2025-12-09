<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Support;

use Hypervel\Cache\Exceptions\RedisCacheException;
use Hypervel\Cache\Redis\Support\CommandErrorHandler;
use Hypervel\Tests\TestCase;
use RedisException;
use RuntimeException;

/**
 * @internal
 * @coversNothing
 */
class CommandErrorHandlerTest extends TestCase
{
    public function testThrowsRedisCacheExceptionForUnknownCommandError(): void
    {
        $original = new RedisException("ERR unknown command 'HSETEX'");

        $this->expectException(RedisCacheException::class);
        $this->expectExceptionMessage('Union tagging requires Redis 8.0+');

        CommandErrorHandler::handle($original);
    }

    public function testThrowsRedisCacheExceptionForErrUnknownError(): void
    {
        $original = new RedisException('ERR unknown subcommand or wrong number of arguments');

        $this->expectException(RedisCacheException::class);
        $this->expectExceptionMessage('Union tagging requires Redis 8.0+');

        CommandErrorHandler::handle($original);
    }

    public function testThrowsRedisCacheExceptionForCommandNotFoundError(): void
    {
        $original = new RedisException('command not found');

        $this->expectException(RedisCacheException::class);
        $this->expectExceptionMessage('Union tagging requires Redis 8.0+');

        CommandErrorHandler::handle($original);
    }

    public function testErrorMessageIsCaseInsensitive(): void
    {
        $original = new RedisException('UNKNOWN COMMAND hsetex');

        $this->expectException(RedisCacheException::class);
        $this->expectExceptionMessage('Union tagging requires Redis 8.0+');

        CommandErrorHandler::handle($original);
    }

    public function testThrowsRedisCacheExceptionForCrossSlotError(): void
    {
        $original = new RedisException('CROSSSLOT Keys in request do not hash to the same slot');

        $this->expectException(RedisCacheException::class);
        $this->expectExceptionMessage('Cross-slot operation attempted');

        CommandErrorHandler::handle($original);
    }

    public function testCrossSlotExceptionContainsBugReportMessage(): void
    {
        $original = new RedisException('CROSSSLOT error');

        try {
            CommandErrorHandler::handle($original);
            $this->fail('Expected RedisCacheException was not thrown');
        } catch (RedisCacheException $e) {
            $this->assertStringContainsString('Please report this issue', $e->getMessage());
            $this->assertSame($original, $e->getPrevious());
        }
    }

    public function testRethrowsUnrelatedExceptions(): void
    {
        $original = new RedisException('Connection refused');

        $this->expectException(RedisException::class);
        $this->expectExceptionMessage('Connection refused');

        CommandErrorHandler::handle($original);
    }

    public function testRethrowsRuntimeExceptions(): void
    {
        $original = new RuntimeException('Some runtime error');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Some runtime error');

        CommandErrorHandler::handle($original);
    }

    public function testExceptionContainsOriginalErrorMessage(): void
    {
        $original = new RedisException("ERR unknown command 'HEXPIRE'");

        try {
            CommandErrorHandler::handle($original);
            $this->fail('Expected RedisCacheException was not thrown');
        } catch (RedisCacheException $e) {
            $this->assertStringContainsString("ERR unknown command 'HEXPIRE'", $e->getMessage());
        }
    }

    public function testExceptionPreservesOriginalExceptionAsPrevious(): void
    {
        $original = new RedisException("ERR unknown command 'HSETEX'");

        try {
            CommandErrorHandler::handle($original);
            $this->fail('Expected RedisCacheException was not thrown');
        } catch (RedisCacheException $e) {
            $this->assertSame($original, $e->getPrevious());
        }
    }

    public function testExceptionMessageContainsHelpfulDiagnosticInfo(): void
    {
        $original = new RedisException("ERR unknown command 'HSETEX'");

        try {
            CommandErrorHandler::handle($original);
            $this->fail('Expected RedisCacheException was not thrown');
        } catch (RedisCacheException $e) {
            $this->assertStringContainsString('Redis 8.0+', $e->getMessage());
            $this->assertStringContainsString('Valkey 9.0+', $e->getMessage());
            $this->assertStringContainsString('phpredis extension 6.3.0+', $e->getMessage());
            $this->assertStringContainsString('HSETEX, HEXPIRE', $e->getMessage());
        }
    }
}
