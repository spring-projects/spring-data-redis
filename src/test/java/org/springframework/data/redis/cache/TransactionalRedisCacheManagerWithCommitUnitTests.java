/*
 * Copyright 2014-2016 the original author or authors.
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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.TransactionalRedisCacheManagerTestBase.BarRepository;
import org.springframework.data.redis.cache.TransactionalRedisCacheManagerTestBase.FooService;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Christoph Strobl
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
@Transactional(transactionManager = "transactionManager")
public class TransactionalRedisCacheManagerWithCommitUnitTests {

	@SuppressWarnings("rawtypes") //
	protected @Autowired RedisTemplate redisTemplate;
	protected @Autowired FooService transactionalService;

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

		@Bean
		public FooService fooService() {
			return new FooService();
		}

		@Bean
		public BarRepository barRepository() {
			return new BarRepository();
		}
	}

	@AfterTransaction
	public void assertThatValuesHaveBeenAddedToRedis() {

		ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);

		verify(redisTemplate.getConnectionFactory().getConnection(), times(1)).zAdd(keyCaptor.capture(), eq(0D),
				valueCaptor.capture());

		Assert.assertThat(new StringRedisSerializer().deserialize(keyCaptor.getValue()).toString(),
				IsEqual.equalTo("bar~keys"));
	}

	/**
	 * @see DATAREDIS-246
	 */
	@Rollback(false)
	@Test
	public void testValuesAddedToCacheWhenTransactionIsCommited() {
		transactionalService.foo();
	}
}
