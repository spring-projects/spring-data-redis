/*
 * Copyright 2011-2018 the original author or authors.
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.StringRedisConnection;

/**
 * @author Costin Leau
 */
public class SessionTest {

	@Test
	public void testSession() throws Exception {
		final RedisConnection conn = mock(RedisConnection.class);
		final StringRedisConnection stringConn = mock(StringRedisConnection.class);
		RedisConnectionFactory factory = mock(RedisConnectionFactory.class);
		final StringRedisTemplate template = spy(new StringRedisTemplate(factory));
		when(factory.getConnection()).thenReturn(conn);
		doReturn(stringConn).when(template).preProcessConnection(eq(conn), anyBoolean());

		template.execute(new SessionCallback<Object>() {
			@SuppressWarnings("rawtypes")
			public Object execute(RedisOperations operations) {
				checkConnection(template, stringConn);
				template.discard();
				assertSame(template, operations);
				checkConnection(template, stringConn);
				return null;
			}
		});
	}

	private void checkConnection(RedisTemplate<?, ?> template, final RedisConnection expectedConnection) {
		template.execute(connection -> {
			assertSame(expectedConnection, connection);
			return null;
		}, true);
	}
}
