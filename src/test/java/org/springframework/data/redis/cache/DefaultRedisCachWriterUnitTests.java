/*
 * Copyright 2017-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


import org.springframework.data.redis.connection.SetCondition;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.redis.connection.ReactiveKeyCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;

/**
 * Unit tests for {@link DefaultRedisCacheWriter}
 *
 * @author John Blum
 * @author Christoph Strobl
 * @author Cobi Eun
 */
@ExtendWith(MockitoExtension.class)
class DefaultRedisCacheWriterUnitTests {

	@Mock private CacheStatisticsCollector mockCacheStatisticsCollector = mock(CacheStatisticsCollector.class);

	@Mock private RedisConnection mockConnection;

	@Mock(strictness = Mock.Strictness.LENIENT) private RedisConnectionFactory mockConnectionFactory;

	@BeforeEach
	void setup() {
		doReturn(this.mockConnection).when(this.mockConnectionFactory).getConnection();
	}

	private RedisCacheWriter newRedisCacheWriter() {
		return spy(new DefaultRedisCacheWriter(this.mockConnectionFactory, mock(BatchStrategy.class))
				.withStatisticsCollector(this.mockCacheStatisticsCollector));
	}

	@Test // GH-2351
	void getWithNonNullTtl() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		Duration ttl = Duration.ofSeconds(15);
		Expiration expiration = Expiration.from(ttl);

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn(value).when(mockStringCommands).getEx(any(), any());

		RedisCacheWriter cacheWriter = newRedisCacheWriter();

		assertThat(cacheWriter.get("TestCache", key, ttl)).isEqualTo(value);

		verify(this.mockConnection, times(1)).stringCommands();
		verify(mockStringCommands, times(1)).getEx(eq(key), eq(expiration));
		verify(this.mockConnection, times(1)).close();
		verifyNoMoreInteractions(this.mockConnection, mockStringCommands);
	}

	@Test // GH-2351
	void getWithNullTtl() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn(value).when(mockStringCommands).get(any());

		RedisCacheWriter cacheWriter = newRedisCacheWriter();

		assertThat(cacheWriter.get("TestCache", key, null)).isEqualTo(value);

		verify(this.mockConnection, times(1)).stringCommands();
		verify(mockStringCommands, times(1)).get(eq(key));
		verify(this.mockConnection, times(1)).close();
		verifyNoMoreInteractions(this.mockConnection, mockStringCommands);
	}

	@Test // GH-2890
	void mustNotUnlockWhenLockingFails() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);
		RedisKeyCommands mockKeyCommands = mock(RedisKeyCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn(mockKeyCommands).when(this.mockConnection).keyCommands();
		doThrow(new PessimisticLockingFailureException("you-shall-not-pass")).when(mockStringCommands)
				.set(any(byte[].class), any(byte[].class), any(SetCondition.class), any());

		RedisCacheWriter cacheWriter = spy(
				new DefaultRedisCacheWriter(this.mockConnectionFactory, Duration.ofMillis(10), mock(BatchStrategy.class))
						.withStatisticsCollector(this.mockCacheStatisticsCollector));

		assertThatException()
				.isThrownBy(() -> cacheWriter.get("TestCache", key, () -> value, Duration.ofMillis(10), false));

		verify(mockKeyCommands, never()).del(any());
	}

	@Test // GH-3450
	void asyncStoreMustRetryLockUntilAcquiredAndOnlyThenWriteAndUnlock() throws Exception {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();
		RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class,
				withSettings().extraInterfaces(ReactiveRedisConnectionFactory.class));
		ReactiveRedisConnection connection = mock(ReactiveRedisConnection.class);
		ReactiveStringCommands strings = mock(ReactiveStringCommands.class);
		ReactiveKeyCommands keys = mock(ReactiveKeyCommands.class);

		doReturn(connection).when((ReactiveRedisConnectionFactory) connectionFactory).getReactiveConnection();
		doReturn(strings).when(connection).stringCommands();
		doReturn(keys).when(connection).keyCommands();
		doReturn(Mono.empty()).when(connection).closeLater();
		doReturn(Mono.just(false), Mono.just(false), Mono.just(true)).when(strings).set(any(), any(),
				any(SetCondition.class), any(Expiration.class));
		doReturn(Mono.just(true)).when(strings).set(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
		doReturn(Mono.just(1L)).when(keys).del(any(ByteBuffer.class));

		RedisCacheWriter writer = RedisCacheWriter.create(connectionFactory,
				it -> it.enableLocking(lock -> lock.sleepTime(Duration.ofMillis(10))));
		writer.store("TestCache", key, value, Duration.ZERO).get(5, TimeUnit.SECONDS);

		InOrder order = inOrder(strings, keys);
		order.verify(strings, times(3)).set(eq(ByteBuffer.wrap("TestCache~lock".getBytes())),
				eq(ByteBuffer.wrap(new byte[0])), eq(SetCondition.ifAbsent()), any(Expiration.class));
		order.verify(strings).set(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
		order.verify(keys).del(ByteBuffer.wrap("TestCache~lock".getBytes()));
		order.verifyNoMoreInteractions();
	}

	@Test // GH-3450
	void asyncStoreMustNotWriteOrUnlockWhenLockingFails() {

		RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class,
				withSettings().extraInterfaces(ReactiveRedisConnectionFactory.class));
		ReactiveRedisConnection connection = mock(ReactiveRedisConnection.class);
		ReactiveStringCommands strings = mock(ReactiveStringCommands.class);
		PessimisticLockingFailureException failure = new PessimisticLockingFailureException("you-shall-not-pass");

		doReturn(connection).when((ReactiveRedisConnectionFactory) connectionFactory).getReactiveConnection();
		doReturn(strings).when(connection).stringCommands();
		doReturn(Mono.empty()).when(connection).closeLater();
		doReturn(Mono.error(failure)).when(strings).set(any(), any(), any(SetCondition.class), any(Expiration.class));

		RedisCacheWriter writer = RedisCacheWriter.create(connectionFactory,
				it -> it.enableLocking(lock -> lock.sleepTime(Duration.ofMillis(10))));

		assertThatException().isThrownBy(() -> writer.store("TestCache", "key".getBytes(), "value".getBytes(), Duration.ZERO)
				.get(5, TimeUnit.SECONDS)).withCause(failure);

		verify(strings).set(any(), any(), any(SetCondition.class), any(Expiration.class));
		verify(strings, never()).set(any(ByteBuffer.class), any(ByteBuffer.class));
		verify(connection, never()).keyCommands();
	}

	@Test // GH-3236
	void usesAsyncPutIfPossible() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class,
				withSettings().extraInterfaces(ReactiveRedisConnectionFactory.class));
		ReactiveRedisConnection mockConnection = mock(ReactiveRedisConnection.class);
		ReactiveStringCommands mockStringCommands = mock(ReactiveStringCommands.class);

		doReturn(mockConnection).when((ReactiveRedisConnectionFactory) connectionFactory).getReactiveConnection();
		doReturn(mockStringCommands).when(mockConnection).stringCommands();
		doReturn(Mono.just(value)).when(mockStringCommands).set(any(), any(), any(SetCondition.class), any(Expiration.class));

		RedisCacheWriter cacheWriter = RedisCacheWriter.create(connectionFactory, cfg -> {
			cfg.immediateWrites(false);
		});

		cacheWriter.put("TestCache", key, value, null);

		verify(mockConnection, times(1)).stringCommands();
		verify(mockStringCommands, times(1)).set(eq(ByteBuffer.wrap(key)), any());
	}

	@Test // GH-3236
	void usesBlockingWritesIfConfiguredWithImmediateWritesEnabled() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class,
				withSettings().strictness(Strictness.LENIENT).extraInterfaces(ReactiveRedisConnectionFactory.class));
		ReactiveRedisConnection reactiveMockConnection = mock(ReactiveRedisConnection.class,
				withSettings().strictness(Strictness.LENIENT));
		ReactiveStringCommands reactiveMockStringCommands = mock(ReactiveStringCommands.class,
				withSettings().strictness(Strictness.LENIENT));

		doReturn(reactiveMockConnection).when((ReactiveRedisConnectionFactory) connectionFactory).getReactiveConnection();
		doReturn(reactiveMockStringCommands).when(reactiveMockConnection).stringCommands();

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn(this.mockConnection).when(connectionFactory).getConnection();

		RedisCacheWriter cacheWriter = RedisCacheWriter.create(connectionFactory, cfg -> {
			cfg.immediateWrites(true);
		});

		cacheWriter.put("TestCache", key, value, null);

		verify(this.mockConnection, times(1)).stringCommands();
		verify(mockStringCommands, times(1)).set(eq(key), any());
		verify(reactiveMockConnection, never()).stringCommands();
		verify(reactiveMockStringCommands, never()).set(eq(ByteBuffer.wrap(key)), any());
	}
}
