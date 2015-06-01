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
package org.springframework.data.redis.cache;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @author Christoph Strobl
 */
@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class RedisCacheUnitTests {

	private static final String CACHE_NAME = "foo";
	private static final String PREFIX = "prefix:";
	private static final byte[] PREFIX_BYTES = "prefix:".getBytes();
	private static final byte[] KNOWN_KEYS_SET_NAME_BYTES = (CACHE_NAME + "~keys").getBytes();

	private static final String KEY = "key";
	private static final byte[] KEY_BYTES = KEY.getBytes();
	private static final byte[] KEY_WITH_PREFIX_BYTES = (PREFIX + KEY).getBytes();

	private static final String VALUE = "value";
	private static final byte[] VALUE_BYTES = VALUE.getBytes();

	private static final byte[] NO_PREFIX_BYTES = new byte[] {};
	private static final long EXPIRATION = 1000;

	RedisTemplate<?, ?> templateSpy;
	@Mock RedisSerializer keySerializerMock;
	@Mock RedisSerializer valueSerializerMock;
	@Mock RedisConnectionFactory connectionFactoryMock;
	@Mock RedisConnection connectionMock;

	RedisCache cache;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {

		RedisTemplate template = new RedisTemplate();
		template.setConnectionFactory(connectionFactoryMock);
		template.setKeySerializer(keySerializerMock);
		template.setValueSerializer(valueSerializerMock);
		template.afterPropertiesSet();

		templateSpy = spy(template);

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);

		when(keySerializerMock.serialize(any(byte[].class))).thenReturn(KEY_BYTES);
		when(valueSerializerMock.serialize(any(byte[].class))).thenReturn(VALUE_BYTES);
	}

	/**
	 * @see DATAREDIS-369
	 */
	@Test
	public void putShouldNotKeepTrackOfKnownKeysWhenPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.put(KEY, VALUE);

		verify(connectionMock, times(1)).set(eq(KEY_WITH_PREFIX_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).expire(eq(KEY_WITH_PREFIX_BYTES), eq(EXPIRATION));
		verify(connectionMock, never()).zAdd(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0D), any(byte[].class));
	}

	/**
	 * @see DATAREDIS-369
	 */
	@Test
	public void putShouldKeepTrackOfKnownKeysWhenNoPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.put(KEY, VALUE);

		verify(connectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).expire(eq(KEY_BYTES), eq(EXPIRATION));
		verify(connectionMock, times(1)).zAdd(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0D), eq(KEY_BYTES));
	}

	/**
	 * @see DATAREDIS-369
	 */
	@Test
	public void clearShouldRemoveKeysUsingKnownKeysWhenNoPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.clear();

		verify(connectionMock, times(1)).zRange(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0L), eq(127L));
	}

	/**
	 * @see DATAREDIS-369
	 */
	@Test
	public void clearShouldCallLuaScritpToRemoveKeysWhenPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.clear();

		verify(connectionMock, times(1)).eval(any(byte[].class), eq(ReturnType.INTEGER), eq(0),
				eq((PREFIX + "*").getBytes()));
	}

	/**
	 * @see DATAREDIS-402
	 */
	@Test
	public void putShouldNotExpireKnownKeysSetWhenTtlIsZero() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);
		cache.put(KEY, VALUE);

		verify(connectionMock, never()).expire(eq(KNOWN_KEYS_SET_NAME_BYTES), anyLong());
	}
}
