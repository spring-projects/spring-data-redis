/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.util.ClassUtils.*;

import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cache.Cache;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
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

	public @Rule ExpectedException exception = ExpectedException.none();

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
		when(valueSerializerMock.deserialize(eq(VALUE_BYTES))).thenReturn(VALUE);
	}

	@Test // DATAREDIS-369
	public void putShouldNotKeepTrackOfKnownKeysWhenPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.put(KEY, VALUE);

		verify(connectionMock, times(1)).set(eq(KEY_WITH_PREFIX_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).expire(eq(KEY_WITH_PREFIX_BYTES), eq(EXPIRATION));
		verify(connectionMock, never()).zAdd(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0D), any(byte[].class));
	}

	@Test // DATAREDIS-369
	public void putShouldKeepTrackOfKnownKeysWhenNoPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.put(KEY, VALUE);

		verify(connectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).expire(eq(KEY_BYTES), eq(EXPIRATION));
		verify(connectionMock, times(1)).zAdd(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0D), eq(KEY_BYTES));
	}

	@Test // DATAREDIS-369
	public void clearShouldRemoveKeysUsingKnownKeysWhenNoPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.clear();

		verify(connectionMock, times(1)).zRange(eq(KNOWN_KEYS_SET_NAME_BYTES), eq(0L), eq(127L));
	}

	@Test // DATAREDIS-369
	public void clearShouldCallLuaScriptToRemoveKeysWhenPrefixIsSet() {

		cache = new RedisCache(CACHE_NAME, PREFIX_BYTES, templateSpy, EXPIRATION);
		cache.clear();

		verify(connectionMock, times(1)).eval(any(byte[].class), eq(ReturnType.INTEGER), eq(0),
				eq((PREFIX + "*").getBytes()));
	}

	@Test // DATAREDIS-402
	public void putShouldNotExpireKnownKeysSetWhenTtlIsZero() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);
		cache.put(KEY, VALUE);

		verify(connectionMock, never()).expire(eq(KNOWN_KEYS_SET_NAME_BYTES), anyLong());
	}

	@Test // DATAREDIS-542
	public void putIfAbsentShouldExpireWhenValueWasSet() {

		when(connectionMock.setNX(KEY_BYTES, VALUE_BYTES)).thenReturn(true);

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 10L);
		Cache.ValueWrapper valueWrapper = cache.putIfAbsent(KEY, VALUE);

		assertThat(valueWrapper, is(nullValue()));
		verify(connectionMock).setNX(KEY_BYTES, VALUE_BYTES);
		verify(connectionMock).expire(eq(KEY_BYTES), anyLong());
	}

	@Test // DATAREDIS-542
	public void putIfAbsentShouldNotExpireWhenValueWasNotSetAndRedisContainsOtherData() {

		String other = "other";
		when(connectionMock.setNX(KEY_BYTES, VALUE_BYTES)).thenReturn(false);
		when(connectionMock.get(KEY_BYTES)).thenReturn(other.getBytes());
		when(valueSerializerMock.deserialize(eq(other.getBytes()))).thenReturn(other);

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 10L);
		Cache.ValueWrapper valueWrapper = cache.putIfAbsent(KEY, VALUE);

		assertThat(valueWrapper, is(notNullValue()));
		verify(connectionMock, never()).expire(eq(KEY_BYTES), anyLong());
	}

	@Test // DATAREDIS-542
	public void putIfAbsentShouldNotSetExpireWhenValueWasNotSetAndRedisContainsSameData() {

		when(connectionMock.setNX(KEY_BYTES, VALUE_BYTES)).thenReturn(false);
		when(connectionMock.get(KEY_BYTES)).thenReturn(VALUE_BYTES);

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 10L);
		Cache.ValueWrapper valueWrapper = cache.putIfAbsent(KEY, VALUE);

		assertThat(valueWrapper, is(notNullValue()));
		verify(connectionMock, never()).expire(eq(KEY_BYTES), anyLong());
	}

	@Test // DATAREDIS-443
	@SuppressWarnings("unchecked")
	public void getWithCallable() throws ClassNotFoundException {

		if (isPresent("org.springframework.cache.Cache$ValueRetrievalException", getDefaultClassLoader())) {
			exception.expect((Class<? extends Throwable>) forName("org.springframework.cache.Cache$ValueRetrievalException",
					getDefaultClassLoader()));
		} else {
			exception.expect(RedisSystemException.class);
		}

		exception.expectMessage("Value for key 'key' could not be loaded");

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);

		cache.get(KEY, new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				throw new UnsupportedOperationException("Expected exception");
			}
		});
	}

	@Test // DATAREDIS-553
	@SuppressWarnings("unchecked")
	public void getWithCallableShouldStoreNullNotAllowingNull() throws ClassNotFoundException {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L, false);

		cache.get(KEY, new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				return null;
			}
		});

		verify(connectionMock, times(1)).get(eq(KEY_BYTES));
		verify(connectionMock, times(1)).multi();
		verify(connectionMock, times(1)).del(eq(KEY_BYTES));
		verify(connectionMock, times(1)).exec();
	}

	@Test // DATAREDIS-553
	@SuppressWarnings("unchecked")
	public void getWithCallableShouldStoreNullAllowingNull() throws ClassNotFoundException {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L, true);

		cache.get(KEY, new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				return null;
			}
		});

		verify(valueSerializerMock).serialize(isA(NullValue.class));
		verify(connectionMock, times(1)).get(eq(KEY_BYTES));
		verify(connectionMock, times(1)).multi();
		verify(connectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).exec();
	}

	@Test // DATAREDIS-443
	public void getWithCallableShouldReadValueFromCallableAddToCache() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);

		cache.get(KEY, new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				return VALUE;
			}
		});

		verify(connectionMock, times(1)).get(eq(KEY_BYTES));
		verify(connectionMock, times(1)).multi();
		verify(connectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));
		verify(connectionMock, times(1)).exec();
	}

	@Test // DATAREDIS-443
	@SuppressWarnings("unchecked")
	public void getWithCallableShouldNotReadValueFromCallableWhenAlreadyPresent() {

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);
		Callable<Object> callableMock = mock(Callable.class);

		when(connectionMock.exists(KEY_BYTES)).thenReturn(true);
		when(connectionMock.get(KEY_BYTES)).thenReturn(VALUE_BYTES);

		assertThat((String) cache.get(KEY, callableMock), equalTo(VALUE));
		verifyZeroInteractions(callableMock);
	}

	@Test // DATAREDIS-468
	public void noMultiExecForCluster() {

		RedisClusterConnection clusterConnectionMock = mock(RedisClusterConnection.class);
		when(connectionFactoryMock.getConnection()).thenReturn(clusterConnectionMock);

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);

		when(connectionMock.exists(KEY_BYTES)).thenReturn(true);
		when(connectionMock.get(KEY_BYTES)).thenReturn(null).thenReturn(VALUE_BYTES);

		cache.put(KEY, VALUE);

		verify(clusterConnectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));
		verify(clusterConnectionMock, never()).multi();
		verify(clusterConnectionMock, never()).exec();
		verifyZeroInteractions(connectionMock);
	}

	@Test // DATAREDIS-468
	public void getWithCallableForCluster() {

		RedisClusterConnection clusterConnectionMock = mock(RedisClusterConnection.class);
		when(connectionFactoryMock.getConnection()).thenReturn(clusterConnectionMock);

		cache = new RedisCache(CACHE_NAME, NO_PREFIX_BYTES, templateSpy, 0L);

		cache.get(KEY, new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				return VALUE;
			}
		});

		verify(clusterConnectionMock, times(1)).get(eq(KEY_BYTES));
		verify(clusterConnectionMock, times(1)).set(eq(KEY_BYTES), eq(VALUE_BYTES));

		verify(clusterConnectionMock, never()).multi();
		verify(clusterConnectionMock, never()).exec();
		verifyZeroInteractions(connectionMock);
	}

}
