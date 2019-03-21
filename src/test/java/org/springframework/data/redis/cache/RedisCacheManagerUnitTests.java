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
package org.springframework.data.redis.cache;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsNull.*;
import static org.hamcrest.core.IsSame.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.cache.Cache;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisCacheManagerUnitTests {

	private @Mock RedisConnection redisConnectionMock;
	private @Mock RedisConnectionFactory redisConnectionFactoryMock;

	@SuppressWarnings("rawtypes")//
	private RedisTemplate redisTemplate;
	private RedisCacheManager cacheManager;

	@SuppressWarnings("rawtypes")
	@Before
	public void setUp() {

		when(redisConnectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);

		redisTemplate = new RedisTemplate();
		redisTemplate.setConnectionFactory(redisConnectionFactoryMock);
		redisTemplate.afterPropertiesSet();

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.afterPropertiesSet();
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Test
	public void testGetCacheReturnsNewCacheWhenRequestedCacheIsNotAvailable() {

		Cache cache = cacheManager.getCache("not-available");
		assertThat(cache, notNullValue());
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Test
	public void testGetCacheReturnsExistingCacheWhenRequested() {

		Cache cache = cacheManager.getCache("cache");
		assertThat(cacheManager.getCache("cache"), sameInstance(cache));
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Test
	public void testCacheInitSouldNotRequestRemoteKeysByDefault() {
		Mockito.verifyZeroInteractions(redisConnectionMock);
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Test
	public void testCacheInitShouldFetchAllCacheKeysWhenLoadingRemoteCachesOnStartupIsEnabled() {

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setLoadRemoteCachesOnStartup(true);
		cacheManager.afterPropertiesSet();

		ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
		verify(redisConnectionMock, times(1)).keys(captor.capture());
		assertThat(redisTemplate.getKeySerializer().deserialize(captor.getValue()).toString(), is("*~keys"));
	}

	/**
	 * @see DATAREDIS-246
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testCacheInitShouldInitializeRemoteCachesCorrectlyWhenLoadingRemoteCachesOnStartupIsEnabled() {

		Set<byte[]> keys = new HashSet<byte[]>(Arrays.asList(redisTemplate.getKeySerializer()
				.serialize("remote-cache~keys")));
		when(redisConnectionMock.keys(any(byte[].class))).thenReturn(keys);

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setLoadRemoteCachesOnStartup(true);
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCacheNames(), IsCollectionContaining.hasItem("remote-cache"));
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Test
	public void testCacheInitShouldNotInitialzeCachesWhenLoadingRemoteCachesOnStartupIsEnabledAndNoCachesAvailableOnRemoteServer() {

		when(redisConnectionMock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptySet());

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setLoadRemoteCachesOnStartup(true);
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCacheNames().isEmpty(), is(true));
	}

	/**
	 * see DATAREDIS-246
	 */
	@Test
	public void testCacheManagerShouldNotDynamicallyCreateCachesWhenInStaticMode() {

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setCacheNames(Arrays.asList("spring", "data"));
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("redis"), nullValue());
	}

	/**
	 * see DATAREDIS-246
	 */
	@Test
	public void testCacheManagerShouldRetrunRegisteredCacheWhenInStaticMode() {

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setCacheNames(Arrays.asList("spring", "data"));
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("spring"), notNullValue());
	}

	/**
	 * see DATAREDIS-246
	 */
	@Test
	public void testPuttingCacheManagerIntoStaticModeShouldNotRemoveAlreadyRegisteredCaches() {

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.getCache("redis");
		cacheManager.setCacheNames(Arrays.asList("spring", "data"));
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("redis"), notNullValue());
	}

	/**
	 * @see DATAREDIS-283
	 */
	@Test
	public void testRetainConfiguredCachesAfterBeanInitialization() {

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setCacheNames(Arrays.asList("spring", "data"));
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("spring"), notNullValue());
		assertThat(cacheManager.getCache("data"), notNullValue());
	}

	/**
	 * @see DATAREDIS-283
	 */
	@Test
	public void testRetainConfiguredCachesAfterBeanInitializationWithLoadingOfRemoteKeys() {

		Set<byte[]> keys = new HashSet<byte[]>(Arrays.asList(redisTemplate.getKeySerializer()
				.serialize("remote-cache~keys")));
		when(redisConnectionMock.keys(any(byte[].class))).thenReturn(keys);

		cacheManager = new RedisCacheManager(redisTemplate);
		cacheManager.setCacheNames(Arrays.asList("spring", "data"));
		cacheManager.setLoadRemoteCachesOnStartup(true);
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("spring"), notNullValue());
		assertThat(cacheManager.getCache("data"), notNullValue());
		assertThat(cacheManager.getCacheNames(), IsCollectionContaining.hasItem("remote-cache"));
	}
}
