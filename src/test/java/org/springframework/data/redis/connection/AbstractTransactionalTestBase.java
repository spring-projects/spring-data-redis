/*
 * Copyright 2014-2024 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit.jupiter.SpringExtension;
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
@ExtendWith(SpringExtension.class)
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

	@BeforeEach
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
			assertThat(connection.exists(key.getBytes()))
					.as("Values for " + key + " should " + (valuesShouldHaveBeenPersisted ? "" : "NOT ") + "have been found.")
					.isEqualTo(valuesShouldHaveBeenPersisted);
		}
		connection.close();
	}

	@Rollback(true)
	@Test // DATAREDIS-73
	public void valueOperationSetShouldBeRolledBackCorrectly() {

		for (String key : KEYS) {
			template.opsForValue().set(key, key + "-value");
		}
	}

	@Rollback(false)
	@Test // DATAREDIS-73
	public void valueOperationSetShouldBeCommittedCorrectly() {

		this.valuesShouldHaveBeenPersisted = true;
		for (String key : KEYS) {
			template.opsForValue().set(key, key + "-value");
		}
	}

	@Rollback(false)
	@Test // GH-2886
	public void shouldReturnReadOnlyCommandResultInTransaction() {

		RedisTemplate<String, String> template = new RedisTemplate<>();
		template.setConnectionFactory(factory);
		template.setEnableTransactionSupport(true);
		template.afterPropertiesSet();

		assertThat(template.hasKey("foo")).isFalse();
	}

	@Test // DATAREDIS-548
	@Transactional(readOnly = true)
	public void valueOperationShouldWorkWithReadOnlyTransactions() {

		this.valuesShouldHaveBeenPersisted = false;
		for (String key : KEYS) {
			template.opsForValue().get(key);
		}
	}

	@Rollback
	@Test // DATAREDIS-73, DATAREDIS-1063
	public void listOperationLPushShoudBeRolledBackCorrectly() {

		for (String key : KEYS) {
			template.opsForList().leftPushAll(key, KEYS.toArray(new String[0]));
		}
	}

	@Rollback(false)
	@Test // DATAREDIS-73, DATAREDIS-1063
	public void listOperationLPushShouldBeCommittedCorrectly() {

		this.valuesShouldHaveBeenPersisted = true;
		for (String key : KEYS) {
			template.opsForList().leftPushAll(key, KEYS.toArray(new String[0]));
		}
	}
}
