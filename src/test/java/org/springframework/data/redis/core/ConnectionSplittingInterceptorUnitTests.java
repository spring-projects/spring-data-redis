/*
 * Copyright 2014-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils.ConnectionSplittingInterceptor;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConnectionSplittingInterceptorUnitTests {

	private static final Method WRITE_METHOD, READONLY_METHOD;

	private ConnectionSplittingInterceptor interceptor;

	private @Mock RedisConnectionFactory connectionFactoryMock;

	private @Mock RedisConnection freshConnectionMock;

	private @Mock RedisConnection boundConnectionMock;

	static {
		try {
			WRITE_METHOD = ClassUtils.getMethod(RedisConnection.class, "expire", byte[].class, long.class);
			READONLY_METHOD = ClassUtils.getMethod(RedisConnection.class, "keys", byte[].class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@BeforeEach
	void setUp() {
		interceptor = new ConnectionSplittingInterceptor(connectionFactoryMock);
		when(connectionFactoryMock.getConnection()).thenReturn(freshConnectionMock);
	}

	@Test // DATAREDIS-73
	void interceptorShouldRequestFreshConnectionForReadonlyCommand() throws Throwable {

		interceptor.intercept(boundConnectionMock, READONLY_METHOD, new Object[] { new byte[] {} });
		verify(connectionFactoryMock, times(1)).getConnection();
		verifyNoInteractions(boundConnectionMock);
	}

	@Test // DATAREDIS-73
	void interceptorShouldUseBoundConnectionForWriteOperations() throws Throwable {

		interceptor.intercept(boundConnectionMock, WRITE_METHOD, new Object[] { new byte[] {}, 0L });
		verify(boundConnectionMock, times(1)).expire(any(byte[].class), anyLong());
		verifyNoInteractions(connectionFactoryMock);
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-73
	void interceptorShouldNotWrapException() {

		when(freshConnectionMock.keys(any(byte[].class))).thenThrow(
				InvalidDataAccessApiUsageException.class);

		Assertions.assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(
				() -> interceptor.intercept(boundConnectionMock, READONLY_METHOD, new Object[] { new byte[] {} }));
	}

}
