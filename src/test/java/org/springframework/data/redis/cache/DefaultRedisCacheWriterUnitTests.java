/*
 * Copyright 2023 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;

import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link DefaultRedisCacheWriter}
 *
 * @author John Blum
 */
@ExtendWith(MockitoExtension.class)
class DefaultRedisCacheWriterUnitTests {

	@Mock
	private CacheStatisticsCollector mockCacheStatisticsCollector = mock(CacheStatisticsCollector.class);

	@Mock
	private RedisConnection mockConnection;

	@Mock(strictness = Mock.Strictness.LENIENT)
	private RedisConnectionFactory mockConnectionFactory;

	@Mock
	private ReactiveRedisConnection mockReactiveConnection;

	@Mock(strictness = Mock.Strictness.LENIENT)
	private TestReactiveRedisConnectionFactory mockReactiveConnectionFactory;

	@BeforeEach
	void setup() {
		doReturn(this.mockConnection).when(this.mockConnectionFactory).getConnection();
		doReturn(this.mockConnection).when(this.mockReactiveConnectionFactory).getConnection();
		doReturn(this.mockReactiveConnection).when(this.mockReactiveConnectionFactory).getReactiveConnection();
	}

	private RedisCacheWriter newRedisCacheWriter() {
		return new DefaultRedisCacheWriter(this.mockConnectionFactory, mock(BatchStrategy.class))
				.withStatisticsCollector(this.mockCacheStatisticsCollector);
	}

	private RedisCacheWriter newReactiveRedisCacheWriter() {
		return newReactiveRedisCacheWriter(Duration.ZERO);
	}

	private RedisCacheWriter newReactiveRedisCacheWriter(Duration sleepTime) {
		return new DefaultRedisCacheWriter(this.mockReactiveConnectionFactory, sleepTime, mock(BatchStrategy.class))
				.withStatisticsCollector(this.mockCacheStatisticsCollector);
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
		verify(this.mockConnection).close();
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
		verify(this.mockConnection).close();
		verifyNoMoreInteractions(this.mockConnection, mockStringCommands);
	}

	@Test // GH-2650
	@SuppressWarnings("all")
	void retrieveWithNoCacheName() {

		byte[] key = "TestKey".getBytes();

		RedisCacheWriter cacheWriter = newReactiveRedisCacheWriter();

		assertThatIllegalArgumentException()
			.isThrownBy(() -> cacheWriter.retrieve(null, key))
			.withMessage("Name must not be null")
			.withNoCause();

		verifyNoInteractions(this.mockReactiveConnectionFactory);
	}

	@Test // GH-2650
	@SuppressWarnings("all")
	void retrieveWithNoKey() {

		RedisCacheWriter cacheWriter = newReactiveRedisCacheWriter();

		assertThatIllegalArgumentException()
			.isThrownBy(() -> cacheWriter.retrieve("TestCacheName", null))
			.withMessage("Key must not be null")
			.withNoCause();

		verifyNoInteractions(this.mockReactiveConnectionFactory);
	}

	@Test // GH-2650
	void retrieveReturnsAsyncFutureWithValue() throws Exception {

		byte[] key = "TestKey".getBytes();

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn("test".getBytes()).when(mockStringCommands).get(any(byte[].class));

		RedisCacheWriter cacheWriter = newRedisCacheWriter();

		CompletableFuture<byte[]> result = cacheWriter.retrieve("TestCacheName", key);

		assertThat(result).isNotNull();
		verifyNoInteractions(this.mockCacheStatisticsCollector);

		byte[] value = result.get();

		assertThat(value).isNotNull();
		assertThat(new String(value)).isEqualTo("test");

		verify(mockStringCommands, times(1)).get(eq(key));
		verify(this.mockCacheStatisticsCollector, times(1)).incGets(eq("TestCacheName"));
		verify(this.mockCacheStatisticsCollector, times(1)).incHits(eq("TestCacheName"));
		verifyNoMoreInteractions(mockStringCommands, this.mockCacheStatisticsCollector);
	}

	@Test // GH-2650
	void retrieveWithExpirationReturnsAsyncFutureWithValue() throws Exception {

		byte[] key = "TestKey".getBytes();

		Duration thirtySeconds = Duration.ofSeconds(30L);

		RedisStringCommands mockStringCommands = mock(RedisStringCommands.class);

		doReturn(mockStringCommands).when(this.mockConnection).stringCommands();
		doReturn("test".getBytes()).when(mockStringCommands).getEx(any(byte[].class), any(Expiration.class));

		RedisCacheWriter cacheWriter = newRedisCacheWriter();

		CompletableFuture<byte[]> result = cacheWriter.retrieve("TestCacheName", key, thirtySeconds);

		assertThat(result).isNotNull();

		byte[] value = result.get();

		assertThat(value).isNotNull();
		assertThat(new String(value)).isEqualTo("test");

		verify(mockStringCommands, times(1)).getEx(eq(key), eq(Expiration.from(thirtySeconds)));
		verify(this.mockCacheStatisticsCollector, times(1)).incGets(eq("TestCacheName"));
		verify(this.mockCacheStatisticsCollector, times(1)).incHits(eq("TestCacheName"));
		verifyNoMoreInteractions(mockStringCommands, this.mockCacheStatisticsCollector);
	}

	@Test // GH-2650
	void retrieveReturnsReactiveFutureWithValue() throws Exception {

		byte[] key = "TestKey".getBytes();

		Duration sixtySeconds = Duration.ofMillis(60L);

		RedisKeyCommands mockKeyCommands = mock(RedisKeyCommands.class);
		ReactiveStringCommands mockStringCommands = mock(ReactiveStringCommands.class);

		doReturn(mockKeyCommands).when(this.mockConnection).keyCommands();
		doReturn(false).when(mockKeyCommands).exists(any(byte[].class));
		doReturn(mockStringCommands).when(this.mockReactiveConnection).stringCommands();
		doReturn(Mono.just(ByteBuffer.wrap("test".getBytes()))).when(mockStringCommands).get(any(ByteBuffer.class));

		RedisCacheWriter cacheWriter = newReactiveRedisCacheWriter(sixtySeconds);

		CompletableFuture<byte[]> result = cacheWriter.retrieve("TestCacheName", key);

		assertThat(result).isNotNull();

		byte[] value = result.get();

		assertThat(value).isNotNull();
		assertThat(new String(value)).isEqualTo("test");

		verify(mockKeyCommands, times(1)).exists(any(byte[].class));
		verify(mockStringCommands, times(1)).get(eq(ByteBuffer.wrap(key)));
		verify(this.mockCacheStatisticsCollector, times(1)).incGets(eq("TestCacheName"));
		verify(this.mockCacheStatisticsCollector, times(1)).incHits(eq("TestCacheName"));
		verifyNoMoreInteractions(mockKeyCommands, mockStringCommands, this.mockCacheStatisticsCollector);
	}

	@Test // GH-2650
	void retrieveReturnsReactiveFutureWithNoValue() throws Exception {

		byte[] key = "TestKey".getBytes();

		RedisKeyCommands mockKeyCommands = mock(RedisKeyCommands.class);
		ReactiveStringCommands mockStringCommands = mock(ReactiveStringCommands.class);

		doReturn(mockKeyCommands).when(this.mockConnection).keyCommands();
		doReturn(false).when(mockKeyCommands).exists(any(byte[].class));
		doReturn(mockStringCommands).when(this.mockReactiveConnection).stringCommands();
		doReturn(Mono.empty()).when(mockStringCommands).get(any(ByteBuffer.class));

		RedisCacheWriter cacheWriter = newReactiveRedisCacheWriter();

		CompletableFuture<byte[]> result = cacheWriter.retrieve("TestCacheName", key);

		assertThat(result).isNotNull();

		byte[] value = result.get();

		assertThat(value).isNull();

		verify(mockKeyCommands, times(1)).exists(any(byte[].class));
		verify(mockStringCommands, times(1)).get(eq(ByteBuffer.wrap(key)));
		verify(this.mockCacheStatisticsCollector, times(1)).incGets(eq("TestCacheName"));
		verify(this.mockCacheStatisticsCollector, times(1)).incMisses(eq("TestCacheName"));
		verifyNoMoreInteractions(mockKeyCommands, mockStringCommands, this.mockCacheStatisticsCollector);
	}

	@Test // GH-2650
	void retrieveWithExpirationReturnsReactiveFutureWithValue() throws Exception {

		byte[] key = "TestKey".getBytes();

		Duration twoMinutes = Duration.ofMinutes(2L);

		RedisKeyCommands mockKeyCommands = mock(RedisKeyCommands.class);
		ReactiveStringCommands mockStringCommands = mock(ReactiveStringCommands.class);

		doReturn(mockKeyCommands).when(this.mockConnection).keyCommands();
		doReturn(false).when(mockKeyCommands).exists(any(byte[].class));
		doReturn(mockStringCommands).when(this.mockReactiveConnection).stringCommands();
		doReturn(Mono.just(ByteBuffer.wrap("test".getBytes()))).when(mockStringCommands).getEx(any(ByteBuffer.class), any());

		RedisCacheWriter cacheWriter = newReactiveRedisCacheWriter();

		CompletableFuture<byte[]> result = cacheWriter.retrieve("TestCacheName", key, twoMinutes);

		assertThat(result).isNotNull();

		byte[] value = result.get();

		assertThat(value).isNotNull();
		assertThat(new String(value)).isEqualTo("test");

		verify(mockKeyCommands, times(1)).exists(any(byte[].class));
		verify(mockStringCommands, times(1)).getEx(eq(ByteBuffer.wrap(key)), eq(Expiration.from(twoMinutes)));
		verify(this.mockCacheStatisticsCollector, times(1)).incGets(eq("TestCacheName"));
		verify(this.mockCacheStatisticsCollector, times(1)).incHits(eq("TestCacheName"));
		verifyNoMoreInteractions(mockKeyCommands, mockStringCommands, this.mockCacheStatisticsCollector);
	}

	interface TestReactiveRedisConnectionFactory extends ReactiveRedisConnectionFactory, RedisConnectionFactory { }

}
