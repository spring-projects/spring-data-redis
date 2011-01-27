/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;

/**
 * @author Costin Leau
 */
public class SessionTest {

	@Test
	public void testSession() throws Exception {
		final RedisConnection conn = mock(RedisConnection.class);
		RedisConnectionFactory factory = mock(RedisConnectionFactory.class);

		when(factory.getConnection()).thenReturn(conn);
		final StringRedisTemplate template = new StringRedisTemplate(factory);

		template.execute(new SessionCallback() {
			@Override
			public Object execute(RedisOperations operations) {
				checkConnection(template, conn);
				template.discard();
				assertSame(template, operations);
				checkConnection(template, conn);
				return null;
			}
		});
	}

	private void checkConnection(RedisTemplate template, final RedisConnection expectedConnection) {
		template.execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				assertSame(expectedConnection, connection);
				return null;
			}
		}, true);
	}
}