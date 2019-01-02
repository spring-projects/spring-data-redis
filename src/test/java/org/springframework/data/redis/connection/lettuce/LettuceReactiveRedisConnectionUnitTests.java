/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.RedisConnectionFailureException;

/**
 * Unit tests for {@link LettuceReactiveRedisConnection}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class LettuceReactiveRedisConnectionUnitTests {

	@Mock(answer = Answers.RETURNS_MOCKS) StatefulRedisConnection<ByteBuffer, ByteBuffer> sharedConnection;

	@Mock RedisReactiveCommands<ByteBuffer, ByteBuffer> reactiveCommands;
	@Mock LettuceConnectionProvider connectionProvider;

	@Before
	public void before() {
		when(connectionProvider.getConnectionAsync(any())).thenReturn(CompletableFuture.completedFuture(sharedConnection));
		when(connectionProvider.releaseAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
		when(sharedConnection.reactive()).thenReturn(reactiveCommands);
	}

	@Test // DATAREDIS-720
	public void shouldLazilyInitializeConnection() {

		new LettuceReactiveRedisConnection(connectionProvider);

		verifyZeroInteractions(connectionProvider);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldExecuteUsingConnectionProvider() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		StepVerifier.create(connection.execute(cmd -> Mono.just("foo"))).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldExecuteDedicatedUsingConnectionProvider() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		StepVerifier.create(connection.executeDedicated(cmd -> Mono.just("foo"))).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720
	public void shouldExecuteOnSharedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		StepVerifier.create(connection.execute(cmd -> Mono.just("foo"))).expectNext("foo").verifyComplete();

		verifyZeroInteractions(connectionProvider);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldExecuteDedicatedWithSharedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		StepVerifier.create(connection.executeDedicated(cmd -> Mono.just("foo"))).expectNext("foo").verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldOperateOnDedicatedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		StepVerifier.create(connection.getConnection()).expectNextCount(1).verifyComplete();

		verify(connectionProvider).getConnectionAsync(StatefulConnection.class);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldCloseOnlyDedicatedConnection() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(sharedConnection,
				connectionProvider);

		StepVerifier.create(connection.getConnection()).expectNextCount(1).verifyComplete();
		StepVerifier.create(connection.getDedicatedConnection()).expectNextCount(1).verifyComplete();

		connection.close();

		verify(sharedConnection, never()).closeAsync();
		verify(connectionProvider, times(1)).releaseAsync(sharedConnection);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldCloseConnectionOnlyOnce() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		StepVerifier.create(connection.getConnection()).expectNextCount(1).verifyComplete();

		connection.close();
		connection.close();

		verify(connectionProvider, times(1)).releaseAsync(sharedConnection);
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	@SuppressWarnings("unchecked")
	public void multipleCallsInProgressShouldConnectOnlyOnce() throws Exception {

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
	public void shouldPropagateConnectionFailures() {

		reset(connectionProvider);
		when(connectionProvider.getConnectionAsync(any()))
				.thenReturn(LettuceFutureUtils.failed(new RedisConnectionException("something went wrong")));

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		StepVerifier.create(connection.getConnection()).expectError(RedisConnectionFailureException.class).verify();
	}

	@Test // DATAREDIS-720, DATAREDIS-721
	public void shouldRejectCommandsAfterClose() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);
		connection.close();

		StepVerifier.create(connection.getConnection()).expectError(IllegalStateException.class).verify();
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	public void bgReWriteAofShouldRespondCorrectly() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		when(reactiveCommands.bgrewriteaof()).thenReturn(Mono.just("OK"));

		StepVerifier.create(connection.serverCommands().bgReWriteAof()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-667, DATAREDIS-708
	public void bgSaveShouldRespondCorrectly() {

		LettuceReactiveRedisConnection connection = new LettuceReactiveRedisConnection(connectionProvider);

		when(reactiveCommands.bgsave()).thenReturn(Mono.just("OK"));

		StepVerifier.create(connection.serverCommands().bgSave()).expectNextCount(1).verifyComplete();
	}
}
