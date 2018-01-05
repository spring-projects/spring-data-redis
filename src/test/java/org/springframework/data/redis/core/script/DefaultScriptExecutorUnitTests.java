/*
 * Copyright 2014-2018 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultScriptExecutorUnitTests {

	private final DefaultRedisScript<String> SCRIPT = new DefaultRedisScript<>("return KEYS[0]", String.class);

	private StringRedisTemplate template;
	private @Mock RedisConnection redisConnectionMock;
	private @Mock RedisConnectionFactory connectionFactoryMock;

	private DefaultScriptExecutor<String> executor;

	@Before
	public void setUp() {

		when(connectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);
		template = spy(new StringRedisTemplate(connectionFactoryMock));
		template.afterPropertiesSet();

		executor = new DefaultScriptExecutor<>(template);
	}

	@Test // DATAREDIS-347
	public void excuteCheckForPresenceOfScriptViaEvalSha1() {

		when(redisConnectionMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenReturn("FOO".getBytes());

		executor.execute(SCRIPT, null);

		verify(redisConnectionMock, times(1)).evalSha(anyString(), any(ReturnType.class), anyInt());
	}

	@Test // DATAREDIS-347
	public void excuteShouldNotCallEvalWhenSha1Exists() {

		when(redisConnectionMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenReturn("FOO".getBytes());

		executor.execute(SCRIPT, null);

		verify(redisConnectionMock, never()).eval(any(byte[].class), any(ReturnType.class), anyInt());
	}

	@Test // DATAREDIS-347
	public void excuteShouldUseEvalInCaseNoSha1PresentForGivenScript() {

		when(redisConnectionMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenThrow(
				new RedisSystemException("NOSCRIPT No matching script. Please use EVAL.", new Exception()));

		executor.execute(SCRIPT, null);

		verify(redisConnectionMock, times(1)).eval(any(byte[].class), any(ReturnType.class), anyInt());
	}

	@Test(expected = UnsupportedOperationException.class) // DATAREDIS-347
	public void excuteShouldThrowExceptionInCaseEvalShaFailsWithOtherThanRedisSystemException() {

		when(redisConnectionMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenThrow(
				new UnsupportedOperationException("NOSCRIPT No matching script. Please use EVAL.", new Exception()));

		executor.execute(SCRIPT, null);
	}

	@Test(expected = RedisSystemException.class) // DATAREDIS-347
	public void excuteShouldThrowExceptionInCaseEvalShaFailsWithAlthoughTheScriptExists() {

		when(redisConnectionMock.evalSha(anyString(), any(ReturnType.class), anyInt())).thenThrow(
				new RedisSystemException("Found Script but could not execute it.", new Exception()));

		executor.execute(SCRIPT, null);
	}
}
