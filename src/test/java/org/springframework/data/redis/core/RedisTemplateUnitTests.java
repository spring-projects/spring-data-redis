/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;

import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisTemplateUnitTests {

	private RedisTemplate<String, String> template;
	private @Mock RedisConnectionFactory connectionFactoryMock;
	private @Mock RedisConnection redisConnectionMock;
	private @Mock RetryListener retryListenerMock;
	private @Mock RecoveryCallback<?> recoveryCallback;

	@Before
	public void setUp() throws Exception {

		template = new RedisTemplate<String, String>();
		template.setConnectionFactory(connectionFactoryMock);
		when(connectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);

		doReturn(true).when(retryListenerMock).open(any(RetryContext.class), any(RetryCallback.class));
		doNothing().when(retryListenerMock)
				.onError(any(RetryContext.class), any(RetryCallback.class), any(Throwable.class));
		doNothing().when(retryListenerMock).close(any(RetryContext.class), any(RetryCallback.class), any(Throwable.class));

		when(recoveryCallback.recover(any(RetryContext.class))).thenReturn(null);

		template.afterPropertiesSet();
	}

	/**
	 * @see DATAREDIS-277
	 */
	@Test
	public void slaveOfIsDelegatedToConnectionCorrectly() {

		template.slaveOf("127.0.0.1", 1001);
		verify(redisConnectionMock, times(1)).slaveOf(eq("127.0.0.1"), eq(1001));
	}

	/**
	 * @see DATAREDIS-277
	 */
	@Test
	public void slaveOfNoOneIsDelegatedToConnectionCorrectly() {

		template.slaveOfNoOne();
		verify(redisConnectionMock, times(1)).slaveOfNoOne();
	}

	/**
	 * @see DATAREDIS-370
	 */
	@Test
	public void shouldRetryAndRecoverAfter2FailedAttempts() {

		RetryTemplate retry = new RetryTemplate();

		retry.setListeners(new RetryListener[] { retryListenerMock });

		template.setRetryOperations(retry);

		// 1st call
		doThrow(new JedisException("dummy"))
		// 2nd call
				.doThrow(new JedisException("dummy"))
				// 3rd call
				.doNothing().when(redisConnectionMock).set((byte[]) any(), (byte[]) any());

		template.opsForValue().set("a", "b");

		verify(retryListenerMock, times(2))
				.onError(any(RetryContext.class), any(RetryCallback.class), any(Throwable.class));
	}

	/**
	 * @see DATAREDIS-370
	 */
	@Test
	public void shouldRetryAndRecoverAfter3FailedAttemptsByCallingRecoveryCallback() throws Exception {

		RetryTemplate retry = new RetryTemplate();

		retry.setListeners(new RetryListener[] { retryListenerMock });

		template.setRetryOperations(retry);
		template.setRecoveryCallback(recoveryCallback);

		// 1st call
		doThrow(new JedisException("dummy"))
		// 2nd call
				.doThrow(new JedisException("dummy"))
				// 3rd call
				.doThrow(new JedisException("dummy")).when(redisConnectionMock).set((byte[]) any(), (byte[]) any());

		template.opsForValue().set("a", "b");

		verify(retryListenerMock, times(3))
				.onError(any(RetryContext.class), any(RetryCallback.class), any(Throwable.class));
		verify(recoveryCallback, times(1)).recover(any(RetryContext.class));
	}
}
