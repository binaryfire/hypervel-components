<?php

declare(strict_types=1);

namespace Hypervel\Tests\Cache\Redis\Support;

use Hyperf\Redis\Pool\PoolFactory;
use Hyperf\Redis\Pool\RedisPool;
use Hypervel\Cache\Redis\Support\Serialization;
use Hypervel\Cache\Redis\Support\StoreContext;
use Hypervel\Cache\Redis\TagMode;
use Hypervel\Redis\RedisConnection;
use Hypervel\Tests\TestCase;
use Mockery as m;
use Redis;

/**
 * @internal
 * @coversNothing
 */
class SerializationTest extends TestCase
{
    public function testSerializeReturnsRawValueWhenSerializerConfigured(): void
    {
        $serialization = $this->createSerialization(serialized: true);

        $this->assertSame('test-value', $serialization->serialize('test-value'));
        $this->assertSame(123, $serialization->serialize(123));
        $this->assertSame(['foo' => 'bar'], $serialization->serialize(['foo' => 'bar']));
    }

    public function testSerializePhpSerializesWhenNoSerializerConfigured(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        $this->assertSame(serialize('test-value'), $serialization->serialize('test-value'));
        $this->assertSame(serialize(['foo' => 'bar']), $serialization->serialize(['foo' => 'bar']));
    }

    public function testSerializeReturnsRawNumericValues(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        // Numeric values are returned raw for performance optimization
        $this->assertSame(123, $serialization->serialize(123));
        $this->assertSame(45.67, $serialization->serialize(45.67));
        $this->assertSame(0, $serialization->serialize(0));
        $this->assertSame(-99, $serialization->serialize(-99));
    }

    public function testSerializeHandlesSpecialFloatValues(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        // INF, -INF, and NaN should be serialized, not returned raw
        $this->assertSame(serialize(INF), $serialization->serialize(INF));
        $this->assertSame(serialize(-INF), $serialization->serialize(-INF));
        // NaN comparison is tricky - it serializes to a special representation
        $result = $serialization->serialize(NAN);
        $this->assertIsString($result);
        $this->assertStringContainsString('NAN', $result);
    }

    public function testUnserializeReturnsNullForNullInput(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        $this->assertNull($serialization->unserialize(null));
    }

    public function testUnserializeReturnsNullForFalseInput(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        $this->assertNull($serialization->unserialize(false));
    }

    public function testUnserializeReturnsRawValueWhenSerializerConfigured(): void
    {
        $serialization = $this->createSerialization(serialized: true);

        $this->assertSame('test-value', $serialization->unserialize('test-value'));
        $this->assertSame(['foo' => 'bar'], $serialization->unserialize(['foo' => 'bar']));
    }

    public function testUnserializePhpUnserializesWhenNoSerializerConfigured(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        $this->assertSame('test-value', $serialization->unserialize(serialize('test-value')));
        $this->assertSame(['foo' => 'bar'], $serialization->unserialize(serialize(['foo' => 'bar'])));
    }

    public function testUnserializeReturnsNumericValuesRaw(): void
    {
        $serialization = $this->createSerialization(serialized: false);

        $this->assertSame(123, $serialization->unserialize(123));
        $this->assertSame(45.67, $serialization->unserialize(45.67));
        // Numeric strings are also returned raw
        $this->assertSame('123', $serialization->unserialize('123'));
        $this->assertSame('45.67', $serialization->unserialize('45.67'));
    }

    public function testSerializeForLuaUsesPackWhenSerializerConfigured(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn(true);
        $connection->shouldReceive('pack')
            ->with(['test-value'])
            ->andReturn(['packed-value']);

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);
        $serialization = new Serialization($context);

        $this->assertSame('packed-value', $serialization->serializeForLua('test-value'));
    }

    public function testSerializeForLuaAppliesCompressionWhenEnabled(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($client);
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_LZF);
        $client->shouldReceive('_serialize')
            ->with(serialize('test-value'))
            ->andReturn('compressed-value');

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);
        $serialization = new Serialization($context);

        $this->assertSame('compressed-value', $serialization->serializeForLua('test-value'));
    }

    public function testSerializeForLuaReturnsPhpSerializedWhenNoSerializerOrCompression(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($client);
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE);

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);
        $serialization = new Serialization($context);

        $this->assertSame(serialize('test-value'), $serialization->serializeForLua('test-value'));
    }

    public function testSerializeForLuaCastsNumericValuesToString(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($client);
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_NONE);

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);
        $serialization = new Serialization($context);

        // Numeric values should be cast to string for Lua ARGV
        $this->assertSame('123', $serialization->serializeForLua(123));
        $this->assertSame('45.67', $serialization->serializeForLua(45.67));
    }

    public function testSerializeForLuaCastsNumericToStringWithCompression(): void
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);
        $client = m::mock(Redis::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn(false);
        $connection->shouldReceive('client')->andReturn($client);
        $client->shouldReceive('getOption')
            ->with(Redis::OPT_COMPRESSION)
            ->andReturn(Redis::COMPRESSION_LZF);
        // When compression is enabled, numeric strings get passed through _serialize
        $client->shouldReceive('_serialize')
            ->with('123')
            ->andReturn('compressed-123');

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);
        $serialization = new Serialization($context);

        $this->assertSame('compressed-123', $serialization->serializeForLua(123));
    }

    private function createSerialization(bool $serialized = false): Serialization
    {
        $poolFactory = m::mock(PoolFactory::class);
        $pool = m::mock(RedisPool::class);
        $connection = m::mock(RedisConnection::class);

        $poolFactory->shouldReceive('getPool')->andReturn($pool);
        $pool->shouldReceive('get')->andReturn($connection);
        $connection->shouldReceive('release');
        $connection->shouldReceive('serialized')->andReturn($serialized);

        $context = new StoreContext($poolFactory, 'default', 'prefix:', TagMode::Any);

        return new Serialization($context);
    }
}
