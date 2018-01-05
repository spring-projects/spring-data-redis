/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.core.script;

import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultReactiveScriptExecutorUnitTests {

	private final DefaultRedisScript<String> SCRIPT = new DefaultRedisScript<>("return KEYS[0]", String.class);

	@Mock ReactiveRedisConnectionFactory connectionFactoryMock;
	@Mock ReactiveRedisConnection connectionMock;
	@Mock ReactiveScriptingCommands scriptingCommandsMock;

	DefaultReactiveScriptExecutor<String> executor;

	@Before
	public void setUp() {

		when(connectionFactoryMock.getReactiveConnection()).thenReturn(connectionMock);
		when(connectionMock.scriptingCommands()).thenReturn(scriptingCommandsMock);

		executor = new DefaultReactiveScriptExecutor<>(connectionFactoryMock, RedisSerializationContext.string());
	}

	@Test // DATAREDIS-683
	public void executeCheckForPresenceOfScriptViaEvalSha1() {

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt()))
				.thenReturn(Flux.just(ByteBuffer.wrap("FOO".getBytes())));

		StepVerifier.create(executor.execute(SCRIPT)).expectNext("FOO").verifyComplete();

		verify(scriptingCommandsMock).evalSha(anyString(), any(ReturnType.class), anyInt());
		verify(scriptingCommandsMock, never()).eval(any(), any(ReturnType.class), anyInt());
	}

	@Test // DATAREDIS-683
	public void executeShouldUseEvalInCaseNoSha1PresentForGivenScript() {

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenReturn(
				Flux.error(new RedisSystemException("NOSCRIPT No matching script. Please use EVAL.", new Exception())));

		when(scriptingCommandsMock.eval(any(), any(ReturnType.class), anyInt()))
				.thenReturn(Flux.just(ByteBuffer.wrap("FOO".getBytes())));

		StepVerifier.create(executor.execute(SCRIPT)).expectNext("FOO").verifyComplete();

		verify(scriptingCommandsMock).evalSha(anyString(), any(ReturnType.class), anyInt());
		verify(scriptingCommandsMock).eval(any(), any(ReturnType.class), anyInt());
	}

	@Test // DATAREDIS-683
	public void executeShouldThrowExceptionInCaseEvalShaFailsWithOtherThanRedisSystemException() {

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenReturn(Flux
				.error(new UnsupportedOperationException("NOSCRIPT No matching script. Please use EVAL.", new Exception())));

		StepVerifier.create(executor.execute(SCRIPT)).expectError(UnsupportedOperationException.class).verify();
	}

	@Test // DATAREDIS-683
	public void releasesConnectionAfterExecution() {

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt()))
				.thenReturn(Flux.just(ByteBuffer.wrap("FOO".getBytes())));

		Flux<String> execute = executor.execute(SCRIPT, Collections.emptyList());

		verify(connectionMock, never()).close();

		StepVerifier.create(execute).expectNext("FOO").verifyComplete();

		verify(connectionMock).close();
	}

	@Test // DATAREDIS-683
	public void releasesConnectionOnError() {

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt()))
				.thenReturn(Flux.error(new RuntimeException()));

		StepVerifier.create(executor.execute(SCRIPT)).expectError().verify();

		verify(connectionMock).close();
	}

	@Test // DATAREDIS-683
	public void doesNotConvertRawResult() {

		Person returnValue = new Person();

		when(scriptingCommandsMock.evalSha(anyString(), any(ReturnType.class), anyInt()))
				.thenReturn(Flux.just(returnValue));

		StepVerifier.create(executor.execute(RedisScript.of("return KEYS[0]"))).expectNext(returnValue).verifyComplete();
	}
}
