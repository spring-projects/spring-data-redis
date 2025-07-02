/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.redis.support.atomic;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link CompareAndSet}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ParameterizedClass
@MethodSource("testParams")
public class CompareAndSetIntegrationIntegrationTests {

	private static final String KEY = "key";

	private final RedisConnectionFactory factory;
	private final RedisTemplate<String, Long> template;
	private final ValueOperations<String, Long> valueOps;

	public CompareAndSetIntegrationIntegrationTests(RedisConnectionFactory factory) {

		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		this.template.afterPropertiesSet();

		this.valueOps = this.template.opsForValue();
	}

	public static Collection<Object[]> testParams() {
		return AtomicCountersParam.testParams();
	}

	@BeforeEach
	void setUp() {

		RedisConnection connection = factory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@Test
	// DATAREDIS-843
	void shouldUpdateCounter() {

		long expected = 5;
		long actual = 5;
		long update = 6;

		CompareAndSet<Long> cas = new CompareAndSet<>(() -> actual, newValue -> valueOps.set(KEY, newValue), KEY, expected,
				update);

		assertThat(template.execute(cas)).isTrue();
		assertThat(valueOps.get(KEY)).isEqualTo(update);
	}

	@Test // DATAREDIS-843
	void expectationNotMet() {

		long expected = 5;
		long actual = 7;
		long update = 6;

		CompareAndSet<Long> cas = new CompareAndSet<>(() -> actual, newValue -> valueOps.set(KEY, newValue), KEY, expected,
				update);

		assertThat(template.execute(cas)).isFalse();
		assertThat(valueOps.get(KEY)).isNull();
	}

	@Test // DATAREDIS-843
	void concurrentUpdate() {

		long expected = 5;
		long actual = 5;
		long update = 6;
		long concurrentlyUpdated = 7;

		CompareAndSet<Long> cas = new CompareAndSet<>(() -> actual, newValue -> {

			RedisConnection connection = factory.getConnection();
			connection.set(KEY.getBytes(), Long.toString(concurrentlyUpdated).getBytes());
			connection.close();

			valueOps.set(KEY, newValue);
		}, KEY, expected, update);

		assertThat(template.execute(cas)).isFalse();
		assertThat(valueOps.get(KEY)).isEqualTo(concurrentlyUpdated);
	}
}
