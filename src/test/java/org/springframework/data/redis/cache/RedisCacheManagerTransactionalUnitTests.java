/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Thomas Darimont
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
@Transactional(transactionManager = "transactionManager")
public class RedisCacheManagerTransactionalUnitTests {

	@Autowired protected CacheManager cacheManager;

	private final static String cacheName = "cache-name";

	@Configuration
	@EnableCaching
	public static class Config {

		@Bean
		public PlatformTransactionManager transactionManager() throws SQLException {

			DataSourceTransactionManager txmgr = new DataSourceTransactionManager();
			txmgr.setDataSource(dataSource());
			txmgr.afterPropertiesSet();

			return txmgr;
		}

		@Bean
		public DataSource dataSource() throws SQLException {

			DataSource dataSourceMock = mock(DataSource.class);
			when(dataSourceMock.getConnection()).thenReturn(mock(Connection.class));

			return dataSourceMock;
		}

		@Bean
		public CacheManager cacheManager() {

			RedisCacheManager cacheManager = new RedisCacheManager(redisTemplate());
			cacheManager.setTransactionAware(true);
			cacheManager.setCacheNames(Arrays.asList(cacheName));
			return cacheManager;
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public RedisTemplate redisTemplate() {

			RedisConnection connectionMock = mock(RedisConnection.class);
			RedisConnectionFactory factoryMock = mock(RedisConnectionFactory.class);

			when(factoryMock.getConnection()).thenReturn(connectionMock);

			RedisTemplate template = new RedisTemplate();
			template.setConnectionFactory(factoryMock);

			return template;
		}
	}

	@Test // DATAREDIS-375
	public void testCacheIsNotDecoratedTwiceWithTransactionAwareCacheDecorator() {

		Cache cache = cacheManager.getCache(cacheName);

		// the cache must be decorated with the TransactionAwareCacheDecorator
		assertTrue(cache instanceof TransactionAwareCacheDecorator);
		TransactionAwareCacheDecorator transactionalCache = (TransactionAwareCacheDecorator) cache;

		// get the target cache via reflection
		Object targetCache = new DirectFieldAccessor(transactionalCache).getPropertyValue("targetCache");

		// the target cache must be an instance of RedisCache and not TransactionAwareCacheDecorator
		assertTrue(targetCache instanceof RedisCache);
	}
}
