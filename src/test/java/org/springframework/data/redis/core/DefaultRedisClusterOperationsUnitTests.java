/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisClusterOperationsUnitTests {

	static final RedisNode NODE_1 = new RedisNode("127.0.0.1", 6379).withId("d1861060fe6a534d42d8a19aeb36600e18785e04");

	@Mock RedisConnectionFactory connectionFactory;
	@Mock RedisClusterConnection connection;

	RedisSerializer<String> serializer;

	DefaultRedisClusterOperations<String, String> clusterOps;

	@Before
	public void setUp() {

		when(connectionFactory.getConnection()).thenReturn(connection);

		serializer = new StringRedisSerializer();

		RedisClusterTemplate<String, String> template = new RedisClusterTemplate<String, String>();
		template.setConnectionFactory(connectionFactory);
		template.setValueSerializer(serializer);
		template.setKeySerializer(serializer);
		template.afterPropertiesSet();

		this.clusterOps = new DefaultRedisClusterOperations<String, String>(template);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldDelegateToConnectionCorrectly() {

		Set<byte[]> keys = new HashSet<byte[]>(Arrays.asList(serializer.serialize("key-1"), serializer.serialize("key-2")));
		when(connection.keys(any(RedisNode.class), any(byte[].class))).thenReturn(keys);

		assertThat(clusterOps.keys(NODE_1, "*".getBytes()), hasItems("key-1", "key-2"));
	}
}
