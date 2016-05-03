/*
 * Copyright 2016 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

/**
 * Unit tests for {@link RedisKeyValueAdapter}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueAdapterUnitTests {

	RedisTemplate<?, ?> redisTemplate;
	RedisKeyValueAdapter redisKeyValueAdapter;

	@Mock JedisConnectionFactory jedisConnectionFactoryMock;
	@Mock RedisConnection redisConnectionMock;

	@Before
	public void setUp() throws Exception {

		redisTemplate = new RedisTemplate<Object, Object>();
		redisTemplate.setConnectionFactory(jedisConnectionFactoryMock);
		redisTemplate.afterPropertiesSet();

		when(jedisConnectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);
		when(redisConnectionMock.getConfig("notify-keyspace-events"))
				.thenReturn(Arrays.asList("notify-keyspace-events", "KEA"));

		redisKeyValueAdapter = new RedisKeyValueAdapter(redisTemplate);
	}

	@Test
	public void destroyShouldNotDestroyConnectionFactory() throws Exception {

		redisKeyValueAdapter.destroy();

		verify(jedisConnectionFactoryMock, never()).destroy();
	}
}
