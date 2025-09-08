/*
 * Copyright 2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import org.springframework.context.Lifecycle;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Transactional integration tests for {@link StringRedisTemplate}.
 *
 * @author Christoph Strobl
 */
@ParameterizedClass
@MethodSource("argumentsStream")
class TransactionalStringRedisTemplateTests {

	private RedisConnectionFactory redisConnectionFactory;
	private StringRedisTemplate stringTemplate;

	TransactionalStringRedisTemplateTests(RedisConnectionFactory redisConnectionFactory) {
		this.redisConnectionFactory = redisConnectionFactory;

		if (redisConnectionFactory instanceof Lifecycle lifecycleBean) {
			lifecycleBean.start();
		}
	}

	@BeforeEach
	void beforeEach() {

		stringTemplate = new StringRedisTemplate(redisConnectionFactory);

		// explicitly enable transaction support
		stringTemplate.setEnableTransactionSupport(true);
		stringTemplate.afterPropertiesSet();

		stringTemplate.execute((RedisCallback) con -> {
			con.flushDb();
			return null;
		});
	}

	@AfterEach
	void afterEach() {
		redisConnectionFactory.getConnection().flushAll();
	}

	@Test // GH-3191
	void visibilityDuringManagedTransaction() throws SQLException {

		stringTemplate.opsForSet().add("myset", "outside");

		DataSource ds = mock(DataSource.class);
		Mockito.when(ds.getConnection()).thenReturn(mock(Connection.class));

		DataSourceTransactionManager txMgr = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(txMgr);
		txTemplate.afterPropertiesSet();
		Map<String, Object> result = txTemplate.execute(x -> {

			Map<String, Object> operationAndOutput = new LinkedHashMap<>();
			// visible since set outside of tx
			operationAndOutput.put("isMember(outside)", stringTemplate.opsForSet().isMember("myset", "outside"));

			// add happens inside multi/exec
			operationAndOutput.put("add", stringTemplate.opsForSet().add("myset", "inside"));

			// changes not visible though inside of tx, but command is not part of multi/exec block
			operationAndOutput.put("isMember(inside)", stringTemplate.opsForSet().isMember("myset", "inside"));

			return operationAndOutput;
		});

		assertThat(result).containsEntry("isMember(outside)", true).containsEntry("add", null)
				.containsEntry("isMember(inside)", false);
	}

	static Stream<Arguments> argumentsStream() {

		LettuceConnectionFactory lcf = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration());
		lcf.afterPropertiesSet();

		JedisConnectionFactory jcf = new JedisConnectionFactory(SettingsUtils.standaloneConfiguration());
		jcf.afterPropertiesSet();

		return Stream.of(Arguments.of(lcf), Arguments.of(jcf));
	}

	@AfterParameterizedClassInvocation
	static void afterInvocation(ArgumentsAccessor accessor) {
		Object o = accessor.get(0);
		if (o instanceof Lifecycle lifecycle) {
			lifecycle.stop();
		}
	}

	@BeforeParameterizedClassInvocation
	static void beforeInvocation(ArgumentsAccessor accessor) {
		Object o = accessor.get(0);
		if (o instanceof Lifecycle lifecycle) {
			lifecycle.start();
		}
	}
}
