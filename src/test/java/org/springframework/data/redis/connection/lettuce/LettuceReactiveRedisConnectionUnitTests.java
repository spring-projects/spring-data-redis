/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.ReactiveStreamCommands.AddStreamRecord;
import org.springframework.data.redis.connection.stream.ByteBufferRecord;
import org.springframework.data.redis.connection.stream.MapRecord;

/**
 * Unit tests for {@link LettuceReactiveRedisConnection}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LettuceReactiveRedisConnectionUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS) StatefulRedisConnection<ByteBuffer, ByteBuffer> sharedConnection;

	@Mock RedisReactiveCommands<ByteBuffer, ByteBuffer> reactiveCommands;
	@Mock LettuceConnectionProvider connectionProvider;

	@BeforeEach
	void before() {
		when(connectionProvider.getConnectionAsync(any())).thenReturn(CompletableFuture.completedFuture(sharedConnection));
		when(connectionProvider.releaseAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
		when(sharedConnection.reactive()).thenReturn(reactiveCommands);
	}

	@Test // DATAREDIS-720
	void shouldLazilyInitializeConnection() {

		new LettuceReactiveRedisConnection(connectionProvider);

		verifyNoInteractions(connectionProvider);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldExecuteUsingConnectionProvider() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		connection.execute(cmd -> Mono.just("foo")).as(StepVerifier::create).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldExecuteDedicatedUsingConnectionProvider() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		connection.executeDedicated(cmd -> Mono.just("foo")).as(StepVerifier::create).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720
	void shouldExecuteOnSharedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		connection.execute(cmd -> Mono.just("foo")).as(StepVerifier::create).expectNext("foo").verifyComplete();

		verifyNoInteractions(connectionProvider);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldExecuteDedicatedWithSharedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		connection.executeDedicated(cmd -> Mono.just("foo")).as(StepVerifier::create).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldOperateOnDedicatedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		connection.getConnection().as(StepVerifier::create).expectNextCount(1).verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldCloseOnlyDedicatedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		connection.getConnection().as(StepVerifier::create).expectNextCount(1).verifyComplete();
		connection.getDedicatedConnection().as(StepVerifier::create).expectNextCount(1).verifyComplete();

		connection.close();

		verify(sharedConnection, never()).closeAsync();
		verify(connectionProvider, times(1)).releaseAsync(sharedConnection);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldCloseConnectionOnlyOnce() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		connection.getConnection().as(StepVerifier::create).expectNextCount(1).verifyComplete();

		connection.close();
		connection.close();

		verify(connectionProvider, times(1)).releaseAsync(sharedConnection);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	@SuppressWarnings("unchecked")
	void multipleCallsInProgressShouldConnectOnlyOnce() throws Exception {

		CompletableFuture<StatefulConnection<?, ?>> connectionFuture = new CompletableFuture<>();
		reset(connectionProvider);
		when(connectionProvider.getConnectionAsync(any())).thenReturn(connectionFuture);

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		CompletableFuture<StatefulConnection<ByteBuffer, ByteBuffer>> first = (CompletableFuture) connection.getConnection()
				.toFuture();
		CompletableFuture<StatefulConnection<ByteBuffer, ByteBuffer>> second = (CompletableFuture) connection
				.getConnection().toFuture();

		assertThat(first).isNotDone();
		assertThat(second).isNotDone();

		verify(connectionProvider, times(1)).getConnectionAsync(StatefulConnection.class);

		connectionFuture.complete(sharedConnection);

		first.get(10, TimeUnit.SECONDS);
		second.get(10, TimeUnit.SECONDS);

		assertThat(first).isCompletedWithValue(sharedConnection);
		assertThat(second).isCompletedWithValue(sharedConnection);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldPropagateConnectionFailures() {

		reset(connectionProvider);
		when(connectionProvider.getConnectionAsync(any()))
				.thenReturn(LettuceFutureUtils.failed(new RedisConnectionException("something went wrong")));

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		connection.getConnection().as(StepVerifier::create).expectError(RedisConnectionFailureException.class).verify();
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	void shouldRejectCommandsAfterClose() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);
		connection.close();

		connection.getConnection().as(StepVerifier::create).expectError(IllegalStateException.class).verify();
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	void bgReWriteAofShouldRespondCorrectly() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		when(reactiveCommands.bgrewriteaof()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgReWriteAof().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-667, DATAREDIS-708
	void bgSaveShouldRespondCorrectly() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		when(reactiveCommands.bgsave()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgSave().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-1122
	void xaddShouldHonorMaxlen() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		ArgumentCaptor<XAddArgs> args = ArgumentCaptor.forClass(XAddArgs.class);
		when(reactiveCommands.xadd(any(ByteBuffer.class), args.capture(), anyMap())).thenReturn(Mono.just("1-1"));

		MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> record = MapRecord.create(ByteBuffer.wrap("key".getBytes()),
				Collections.emptyMap());

		connection.streamCommands().xAdd(Mono.just(AddStreamRecord.of(ByteBufferRecord.of(record)).maxlen(100)))
				.subscribe();

		assertThat(args.getValue()).extracting("maxlen").isEqualTo(100L);
	}

	@Test // GH-2047
	void xaddShouldHonorNoMkStream() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		ArgumentCaptor<XAddArgs> args = ArgumentCaptor.forClass(XAddArgs.class);
		when(reactiveCommands.xadd(any(ByteBuffer.class), args.capture(), anyMap())).thenReturn(Mono.just("1-1"));

		MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> record = MapRecord.create(ByteBuffer.wrap("key".getBytes()),
				Collections.emptyMap());

		connection.streamCommands().xAdd(Mono.just(AddStreamRecord.of(ByteBufferRecord.of(record)).makeNoStream()))
				.subscribe();

		assertThat(args.getValue()).extracting("nomkstream").isEqualTo(true);
	}

}
