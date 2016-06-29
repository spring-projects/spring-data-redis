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
package org.springframework.data.redis.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.transaction.AfterTransaction;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

/**
 * Base class with integration tests for transactional use.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@Transactional(transactionManager = "transactionManager")
public abstract class AbstractTransactionalTestBase {

	@Configuration
	public abstract static class RedisContextConfiguration {

		@Bean
		public StringRedisTemplate redisTemplate() {

			StringRedisTemplate template = new StringRedisTemplate(redisConnectionFactory());

			// explicitly enable transaction support
			template.setEnableTransactionSupport(true);
			return template;
		}

		@Bean
		public abstract RedisConnectionFactory redisConnectionFactory();

		@Bean
		public PlatformTransactionManager transactionManager() throws SQLException {
			return new DataSourceTransactionManager(dataSource());
		}

		@Bean
		public DataSource dataSource() throws SQLException {

			DataSource ds = Mockito.mock(DataSource.class);
			Mockito.when(ds.getConnection()).thenReturn(Mockito.mock(Connection.class));
			return ds;
		}
	}

	private @Autowired StringRedisTemplate template;

	private @Autowired RedisConnectionFactory factory;

	private List<String> KEYS = Arrays.asList("spring", "data", "redis");
	private boolean valuesShouldHaveBeenPersisted = false;

	@Before
	public void setUp() {
		valuesShouldHaveBeenPersisted = false;
		cleanDataStore();
	}

	private void cleanDataStore() {

		RedisConnection connection = factory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@AfterTransaction
	public void verifyTransactionResult() {

		RedisConnection connection = factory.getConnection();
		for (String key : KEYS) {
			Assert.assertThat(
					"Values for " + key + " should " + (valuesShouldHaveBeenPersisted ? "" : "NOT ") + "have been found.",
					connection.exists(key.getBytes()), Is.is(valuesShouldHaveBeenPersisted));
		}
		connection.close();
	}

	/**
	 * @see DATAREDIS-73
	 */
	@Rollback(true)
	@Test
	public void valueOperationSetShouldBeRolledBackCorrectly() {

		for (String key : KEYS) {
			template.opsForValue().set(key, key + "-value");
		}
	}

	/**
	 * @see DATAREDIS-73
	 */
	@Rollback(false)
	@Test
	public void valueOperationSetShouldBeCommittedCorrectly() {

		this.valuesShouldHaveBeenPersisted = true;
		for (String key : KEYS) {
			template.opsForValue().set(key, key + "-value");
		}
	}

	/**
	 * @see DATAREDIS-548
	 */
	@Test
	@Transactional(readOnly = true)
	public void valueOperationShouldWorkWithReadOnlyTransactions() {

		this.valuesShouldHaveBeenPersisted = false;
		for (String key : KEYS) {
			template.opsForValue().get(key);
		}
	}

	/**
	 * @see DATAREDIS-73
	 */
	@Rollback(true)
	@Test
	public void listOperationLPushShoudBeRolledBackCorrectly() {

		for (String key : KEYS) {
			template.opsForList().leftPushAll(key, (String[]) KEYS.toArray());
		}
	}

	/**
	 * @see DATAREDIS-73
	 */
	@Rollback(false)
	@Test
	public void listOperationLPushShouldBeCommittedCorrectly() {

		this.valuesShouldHaveBeenPersisted = true;
		for (String key : KEYS) {
			template.opsForList().leftPushAll(key, (String[]) KEYS.toArray());
		}
	}
}
