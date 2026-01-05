/*
 * Copyright 2025-present the original author or authors.
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

import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Transactional integration tests for {@link StringRedisTemplate}.
 *
 * @author Christoph Strobl
 * @author LeeHyungGeol
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@BeforeEach
	void beforeEach() {

		stringTemplate = new StringRedisTemplate(redisConnectionFactory);

		// explicitly enable transaction support
		stringTemplate.setEnableTransactionSupport(true);
		stringTemplate.afterPropertiesSet();

		stringTemplate.execute((RedisCallback) con -> {
			con.serverCommands().flushDb();
			return null;
		});
	}

	@AfterEach
	void afterEach() {
		redisConnectionFactory.getConnection().serverCommands().flushAll();
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

	@Test // GH-3187
	void allRangeWithScoresMethodsShouldExecuteImmediatelyInTransaction() throws SQLException {

		DataSource ds = mock(DataSource.class);
		when(ds.getConnection()).thenReturn(mock(Connection.class));

		DataSourceTransactionManager txMgr = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(txMgr);
		txTemplate.afterPropertiesSet();

		// Add data outside transaction
		stringTemplate.opsForZSet().add("testzset", "outside1", 1.0);
		stringTemplate.opsForZSet().add("testzset", "outside2", 2.0);

		Map<String, Object> result = txTemplate.execute(x -> {
			Map<String, Object> ops = new LinkedHashMap<>();

			// Query data added outside transaction (should execute immediately)
			ops.put("rangeWithScores_outside", stringTemplate.opsForZSet().rangeWithScores("testzset", 0, -1));
			ops.put("reverseRangeWithScores_outside", stringTemplate.opsForZSet().reverseRangeWithScores("testzset", 0, -1));
			ops.put("rangeByScoreWithScores_outside",
					stringTemplate.opsForZSet().rangeByScoreWithScores("testzset", 1.0, 2.0));
			ops.put("reverseRangeByScoreWithScores_outside",
					stringTemplate.opsForZSet().reverseRangeByScoreWithScores("testzset", 1.0, 2.0));

			// Add inside transaction (goes into multi/exec queue)
			ops.put("add_inside", stringTemplate.opsForZSet().add("testzset", "inside", 3.0));

			// Changes made inside transaction should not be visible yet (read executes immediately)
			ops.put("rangeWithScores_inside", stringTemplate.opsForZSet().rangeWithScores("testzset", 0, -1));
			ops.put("reverseRangeWithScores_inside", stringTemplate.opsForZSet().reverseRangeWithScores("testzset", 0, -1));
			ops.put("rangeByScoreWithScores_inside",
					stringTemplate.opsForZSet().rangeByScoreWithScores("testzset", 1.0, 3.0));
			ops.put("reverseRangeByScoreWithScores_inside",
					stringTemplate.opsForZSet().reverseRangeByScoreWithScores("testzset", 1.0, 3.0));

			return ops;
		});

		// add result is null (no result until exec)
		assertThat(result).containsEntry("add_inside", null);

		// changes made outside transaction are visible
		assertThatResultForOperationContainsExactly(result, "rangeWithScores_outside", "outside1", "outside2");
		assertThatResultForOperationContainsExactly(result, "reverseRangeWithScores_outside", "outside2", "outside1");
		assertThatResultForOperationContainsExactly(result, "rangeByScoreWithScores_outside", "outside1", "outside2");
		assertThatResultForOperationContainsExactly(result, "reverseRangeByScoreWithScores_outside", "outside2",
				"outside1");

		// changes made inside transaction are not visible (i.e. a 3rd element was added but not detected in range op)
		assertThatResultForOperationContainsExactly(result, "rangeWithScores_inside", "outside1", "outside2");
		assertThatResultForOperationContainsExactly(result, "reverseRangeWithScores_inside", "outside2", "outside1");
		assertThatResultForOperationContainsExactly(result, "rangeByScoreWithScores_inside", "outside1", "outside2");
		assertThatResultForOperationContainsExactly(result, "reverseRangeByScoreWithScores_inside", "outside2", "outside1");
	}

	private void assertThatResultForOperationContainsExactly(Map<String, Object> result, String operation,
			String... expectedValues) {
		assertThat(result.get(operation)).asInstanceOf(InstanceOfAssertFactories.set(TypedTuple.class))
				.hasSize(expectedValues.length).extracting(TypedTuple::getValue).containsExactly(expectedValues);
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
