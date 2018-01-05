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
package org.springframework.data.redis.core;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils.ConnectionSplittingInterceptor;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class ConnectionSplittingInterceptorUnitTests {

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

	@Before
	public void setUp() {
		interceptor = new ConnectionSplittingInterceptor(connectionFactoryMock);
		Mockito.when(connectionFactoryMock.getConnection()).thenReturn(freshConnectionMock);
	}

	@Test // DATAREDIS-73
	public void interceptorShouldRequestFreshConnectionForReadonlyCommand() throws Throwable {

		interceptor.intercept(boundConnectionMock, READONLY_METHOD, new Object[] { new byte[] {} }, null);
		Mockito.verify(connectionFactoryMock, Mockito.times(1)).getConnection();
		Mockito.verifyZeroInteractions(boundConnectionMock);
	}

	@Test // DATAREDIS-73
	public void interceptorShouldUseBoundConnectionForWriteOperations() throws Throwable {

		interceptor.intercept(boundConnectionMock, WRITE_METHOD, new Object[] { new byte[] {}, 0L }, null);
		Mockito.verify(boundConnectionMock, Mockito.times(1)).expire(Mockito.any(byte[].class), Mockito.anyLong());
		Mockito.verifyZeroInteractions(connectionFactoryMock);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAREDIS-73
	public void interceptorShouldNotWrapException() throws Throwable {

		Mockito.when(freshConnectionMock.keys(Mockito.any(byte[].class))).thenThrow(
				InvalidDataAccessApiUsageException.class);
		interceptor.intercept(boundConnectionMock, READONLY_METHOD, new Object[] { new byte[] {} }, null);
	}

}
