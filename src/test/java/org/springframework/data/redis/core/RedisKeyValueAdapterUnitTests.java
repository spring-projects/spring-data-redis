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
import java.util.Collections;
import java.util.LinkedHashSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.convert.Bucket;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.convert.SimpleIndexedPropertyValue;

/**
 * Unit tests for {@link RedisKeyValueAdapter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
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

	/**
	 * @see DATAREDIS-507
	 */
	@Test
	public void destroyShouldNotDestroyConnectionFactory() throws Exception {

		redisKeyValueAdapter.destroy();

		verify(jedisConnectionFactoryMock, never()).destroy();
	}

	/**
	 * @see DATAREDIS-512
	 */
	@Test
	public void putShouldRemoveExistingIndexValuesWhenUpdating() {

		RedisData rd = new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("_id", "1")));
		rd.addIndexedData(new SimpleIndexedPropertyValue("persons", "firstname", "rand"));

		when(redisConnectionMock.keys(any(byte[].class)))
				.thenReturn(new LinkedHashSet<byte[]>(Arrays.asList("persons:firstname:rand".getBytes())));
		when(redisConnectionMock.del((byte[][]) anyVararg())).thenReturn(1L);

		redisKeyValueAdapter.put("1", rd, "persons");

		verify(redisConnectionMock, times(1)).sRem(any(byte[].class), any(byte[].class));
	}

	/**
	 * @see DATAREDIS-512
	 */
	@Test
	public void putShouldNotTryToRemoveExistingIndexValuesWhenInsertingNew() {

		RedisData rd = new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("_id", "1")));
		rd.addIndexedData(new SimpleIndexedPropertyValue("persons", "firstname", "rand"));

		when(redisConnectionMock.sMembers(any(byte[].class)))
				.thenReturn(new LinkedHashSet<byte[]>(Arrays.asList("persons:firstname:rand".getBytes())));
		when(redisConnectionMock.del((byte[][]) anyVararg())).thenReturn(0L);

		redisKeyValueAdapter.put("1", rd, "persons");

		verify(redisConnectionMock, never()).sRem(any(byte[].class), (byte[][]) anyVararg());
	}
}
