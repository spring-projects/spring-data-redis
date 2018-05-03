/*
 * Copyright 2016. the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultValueOperationsUnitTests<K, V> {

	@Mock RedisConnectionFactory connectionFactoryMock;
	@Mock RedisConnection connectionMock;
	RedisSerializer<String> serializer;
	RedisTemplate<String, V> template;
	ValueOperations<String, V> valueOps;

	@Before
	public void setUp() {

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);

		serializer = new StringRedisSerializer();

		template = new RedisTemplate<String, V>();
		template.setKeySerializer(serializer);
		template.setConnectionFactory(connectionFactoryMock);
		template.afterPropertiesSet();

		this.valueOps = template.opsForValue();
	}

	@Test // DATAREDIS-562
	public void bitfieldShouldBeDelegatedCorrectly() {

		BitFieldSubCommands command = BitFieldSubCommands.create().get(BitFieldType.INT_8).valueAt(0L);

		valueOps.bitField("key", command);

		verify(connectionMock, times(1)).bitField(eq(serializer.serialize("key")), eq(command));
	}
}
