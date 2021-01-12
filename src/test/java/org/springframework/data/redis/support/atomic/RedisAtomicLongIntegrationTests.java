/*
 * Copyright 2013-2021 the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration test of {@link RedisAtomicLong}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Graham MacMaster
 */
@MethodSource("testParams")
public class RedisAtomicLongIntegrationTests {

	private final RedisConnectionFactory factory;
	private final RedisTemplate<String, Long> template;

	private RedisAtomicLong longCounter;

	public RedisAtomicLongIntegrationTests(RedisConnectionFactory factory) {

		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		this.template.afterPropertiesSet();
	}

	public static Collection<Object[]> testParams() {
		return AtomicCountersParam.testParams();
	}

	@BeforeEach
	void before() {

		RedisConnection connection = factory.getConnection();
		connection.flushDb();
		connection.close();

		this.longCounter = new RedisAtomicLong(getClass().getSimpleName() + ":long", factory);
	}

	@ParameterizedRedisTest
	void testCheckAndSet() {

		longCounter.set(0);
		assertThat(longCounter.compareAndSet(1, 10)).isFalse();
		assertThat(longCounter.compareAndSet(0, 10)).isTrue();
		assertThat(longCounter.compareAndSet(10, 0)).isTrue();
	}

	@ParameterizedRedisTest
	void testIncrementAndGet() {

		longCounter.set(0);
		assertThat(longCounter.incrementAndGet()).isOne();
	}

	@ParameterizedRedisTest
	void testAddAndGet() {

		longCounter.set(0);
		long delta = 5;
		assertThat(longCounter.addAndGet(delta)).isEqualTo(delta);
	}

	@ParameterizedRedisTest
	void testDecrementAndGet() {

		longCounter.set(1);
		assertThat(longCounter.decrementAndGet()).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void testGetAndIncrement() {

		longCounter.set(1);
		assertThat(longCounter.getAndIncrement()).isOne();
		assertThat(longCounter.get()).isEqualTo(2);
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void testGetAndAdd() {

		longCounter.set(1);
		assertThat(longCounter.getAndAdd(5)).isOne();
		assertThat(longCounter.get()).isEqualTo(6);
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void testGetAndDecrement() {

		longCounter.set(1);
		assertThat(longCounter.getAndDecrement()).isOne();
		assertThat(longCounter.get()).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void testGetAndSet() {

		longCounter.set(1);
		assertThat(longCounter.getAndSet(5)).isOne();
		assertThat(longCounter.get()).isEqualTo(5);
	}

	@ParameterizedRedisTest
	void testGetExistingValue() {

		longCounter.set(5);
		RedisAtomicLong keyCopy = new RedisAtomicLong(longCounter.getKey(), factory);
		assertThat(longCounter.get()).isEqualTo(keyCopy.get());
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldThrowExceptionIfAtomicLongIsUsedWithRedisTemplateAndNoKeySerializer() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new RedisAtomicLong("foo", new RedisTemplate<>()))
				.withMessageContaining("a valid key serializer in template is required");
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldThrowExceptionIfAtomicLongIsUsedWithRedisTemplateAndNoValueSerializer() {

		RedisTemplate<String, Long> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new RedisAtomicLong("foo", template))
				.withMessageContaining("a valid value serializer in template is required");
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldBeAbleToUseRedisAtomicLongWithProperlyConfiguredRedisTemplate() {

		RedisTemplate<String, Long> template = new RedisTemplate<>();
		template.setConnectionFactory(factory);
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		template.afterPropertiesSet();

		RedisAtomicLong ral = new RedisAtomicLong("DATAREDIS-317.atomicLong", template);
		ral.set(32L);

		assertThat(ral.get()).isEqualTo(32L);
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void getThrowsExceptionWhenKeyHasBeenRemoved() {

		// setup long
		RedisAtomicLong test = new RedisAtomicLong("test", factory, 1);
		assertThat(test.get()).isOne();

		template.delete("test");

		assertThatExceptionOfType(DataRetrievalFailureException.class).isThrownBy(test::get)
				.withMessageContaining("'test' seems to no longer exist");
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup long
		RedisAtomicLong test = new RedisAtomicLong("test", factory, 1);
		assertThat(test.get()).isOne();

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void updateAndGetAppliesGivenUpdateFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		long expectedNewValue = 10;
		longCounter.set(initialValue);

		LongUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		long result = longCounter.updateAndGet(updateFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(longCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void updateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		longCounter.set(initialValue);

		LongUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isEqualTo(initialValue);

			return -1;
		};

		longCounter.updateAndGet(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndUpdateAppliesGivenUpdateFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		long expectedNewValue = 10;
		longCounter.set(initialValue);

		LongUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		long result = longCounter.getAndUpdate(updateFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(longCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndUpdateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		longCounter.set(initialValue);

		LongUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isEqualTo(initialValue);

			return -1;
		};

		longCounter.getAndUpdate(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void accumulateAndGetAppliesGivenAccumulatorFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		long expectedNewValue = 10;
		longCounter.set(initialValue);

		LongBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		long result = longCounter.accumulateAndGet(15L, accumulatorFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(longCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void accumulateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		longCounter.set(initialValue);

		LongBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isEqualTo(initialValue);
			assertThat(y).isEqualTo(15);

			return -1;
		};

		longCounter.accumulateAndGet(15L, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndAccumulateAppliesGivenAccumulatorFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		long expectedNewValue = 10;
		longCounter.set(initialValue);

		LongBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		long result = longCounter.getAndAccumulate(15L, accumulatorFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(longCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndAccumulateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		long initialValue = 5;
		longCounter.set(initialValue);

		LongBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isEqualTo(initialValue);
			assertThat(y).isEqualTo(15);

			return -1;
		};

		longCounter.getAndAccumulate(15L, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}
}
