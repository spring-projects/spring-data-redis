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
package org.springframework.data.redis.support.collections;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractRedisCollectionUnitTests {

	private AbstractRedisCollection<String> collection;

	@SuppressWarnings("rawtypes")//
	private RedisTemplate redisTemplateSpy;
	private @Mock RedisConnectionFactory connectionFactoryMock;
	private @Mock RedisConnection connectionMock;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before
	public void setUp() {

		redisTemplateSpy = spy(new RedisTemplate() {

			public Boolean hasKey(Object key) {
				return false;
			}
		});
		redisTemplateSpy.setConnectionFactory(connectionFactoryMock);
		redisTemplateSpy.afterPropertiesSet();

		collection = new AbstractRedisCollection<String>("key", redisTemplateSpy) {

			private List<String> delegate = new ArrayList<>();

			@Override
			public boolean add(String value) {
				return this.delegate.add(value);
			};

			@Override
			public DataType getType() {
				return DataType.LIST;
			}

			@Override
			public Iterator<String> iterator() {
				return this.delegate.iterator();
			}

			@Override
			public int size() {
				return this.delegate.size();
			}

			@Override
			public boolean isEmpty() {
				return this.delegate.isEmpty();
			}

		};

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-188
	public void testRenameOfEmptyCollectionShouldNotTriggerRedisOperation() {

		collection.rename("new-key");
		verify(redisTemplateSpy, never()).rename(eq("key"), eq("new-key"));
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-188
	public void testRenameCollectionShouldTriggerRedisOperation() {

		when(redisTemplateSpy.hasKey(any())).thenReturn(Boolean.TRUE);

		collection.add("spring-data-redis");
		collection.rename("new-key");
		verify(redisTemplateSpy, times(1)).rename(eq("key"), eq("new-key"));
	}
}
