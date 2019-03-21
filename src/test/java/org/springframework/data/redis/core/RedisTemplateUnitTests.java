/*
 * Copyright 2014 the original author or authors.
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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisTemplateUnitTests {

	private RedisTemplate<String, String> template;
	private @Mock RedisConnectionFactory connectionFactoryMock;
	private @Mock RedisConnection redisConnectionMock;

	@Before
	public void setUp() {

		template = new RedisTemplate<String, String>();
		template.setConnectionFactory(connectionFactoryMock);
		when(connectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);

		template.afterPropertiesSet();
	}

	/**
	 * @see DATAREDIS-277
	 */
	@Test
	public void slaveOfIsDelegatedToConnectionCorrectly() {

		template.slaveOf("127.0.0.1", 1001);
		verify(redisConnectionMock, times(1)).slaveOf(eq("127.0.0.1"), eq(1001));
	}

	/**
	 * @see DATAREDIS-277
	 */
	@Test
	public void slaveOfNoOneIsDelegatedToConnectionCorrectly() {

		template.slaveOfNoOne();
		verify(redisConnectionMock, times(1)).slaveOfNoOne();
	}

}
