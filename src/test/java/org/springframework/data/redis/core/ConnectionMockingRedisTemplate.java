/*
 * Copyright 2021-2024 the original author or authors.
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

import org.mockito.Mockito;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Test extension to {@link RedisTemplate} to use a Mockito mocked {@link RedisConnection}.
 *
 * @author Christoph Strobl
 */
public class ConnectionMockingRedisTemplate<K, V> extends RedisTemplate<K, V> {

	private final RedisConnection connectionMock;

	private ConnectionMockingRedisTemplate() {

		connectionMock = Mockito.mock(RedisConnection.class);

		RedisConnectionFactory connectionFactory = Mockito.mock(RedisConnectionFactory.class);
		Mockito.when(connectionFactory.getConnection()).thenReturn(connectionMock);

		setConnectionFactory(connectionFactory);
	}

	static <K, V> ConnectionMockingRedisTemplate<K, V> template() {
		return builder().build();
	}

	static MockTemplateBuilder builder() {
		return new MockTemplateBuilder();
	}

	public RedisConnection verify() {
		return Mockito.verify(connectionMock);
	}

	public byte[] serializeKey(K key) {
		return ((RedisSerializer<K>) getKeySerializer()).serialize(key);
	}

	public RedisConnection never() {
		return Mockito.verify(connectionMock, Mockito.never());
	}

	public RedisConnection doReturn(Object o) {
		return Mockito.doReturn(o).when(connectionMock);
	}

	public static class MockTemplateBuilder {

		private ConnectionMockingRedisTemplate template = new ConnectionMockingRedisTemplate();

		public <K, V> ConnectionMockingRedisTemplate<K, V> build() {

			template.afterPropertiesSet();
			return template;
		}

	}

}
