/*
 * Copyright 2016-2025 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultValueOperationsUnitTests<K, V> {

	@Mock RedisConnectionFactory connectionFactoryMock;
	@Mock RedisConnection connectionMock;
	private RedisSerializer<String> serializer;
	private RedisTemplate<String, V> template;
	private ValueOperations<String, V> valueOps;

	@BeforeEach
	void setUp() {

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);

		serializer = new StringRedisSerializer();

		template = new RedisTemplate<String, V>();
		template.setKeySerializer(serializer);
		template.setConnectionFactory(connectionFactoryMock);
		template.afterPropertiesSet();

		this.valueOps = template.opsForValue();
	}

	@Test // DATAREDIS-562
	void bitfieldShouldBeDelegatedCorrectly() {

		BitFieldSubCommands command = BitFieldSubCommands.create().get(BitFieldType.INT_8).valueAt(0L);

		valueOps.bitField("key", command);

		verify(connectionMock, times(1)).bitField(eq(serializer.serialize("key")), eq(command));
	}
}
