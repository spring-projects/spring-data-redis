/*
 * Copyright 2013-present the original author or authors.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration test of {@link RedisAtomicInteger}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Graham MacMaster
 */
@ParameterizedClass
@MethodSource("testParams")
public class RedisAtomicIntegerIntegrationTests {

	private final RedisConnectionFactory factory;
	private final RedisTemplate<String, Integer> template;

	private RedisAtomicInteger intCounter;

	public RedisAtomicIntegerIntegrationTests(RedisConnectionFactory factory) {

		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
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

		this.intCounter = new RedisAtomicInteger(getClass().getSimpleName() + ":int", factory);
	}

	@Test
	void testCheckAndSet() {

		intCounter.set(0);
		assertThat(intCounter.compareAndSet(1, 10)).isFalse();
		assertThat(intCounter.compareAndSet(0, 10)).isTrue();
		assertThat(intCounter.compareAndSet(10, 0)).isTrue();
	}

	@Test
	void testIncrementAndGet() {

		intCounter.set(0);
		assertThat(intCounter.incrementAndGet()).isOne();
	}

	@Test
	void testAddAndGet() {

		intCounter.set(0);
		int delta = 5;
		assertThat(intCounter.addAndGet(delta)).isEqualTo(delta);
	}

	@Test
	void testDecrementAndGet() {

		intCounter.set(1);
		assertThat(intCounter.decrementAndGet()).isZero();
	}

	@Test // DATAREDIS-469
	void testGetAndIncrement() {

		intCounter.set(1);
		assertThat(intCounter.getAndIncrement()).isOne();
		assertThat(intCounter.get()).isEqualTo(2);
	}

	@Test // DATAREDIS-469
	void testGetAndAdd() {

		intCounter.set(1);
		assertThat(intCounter.getAndAdd(5)).isOne();
		assertThat(intCounter.get()).isEqualTo(6);
	}

	@Test // DATAREDIS-469
	void testGetAndDecrement() {

		intCounter.set(1);
		assertThat(intCounter.getAndDecrement()).isOne();
		assertThat(intCounter.get()).isZero();
	}

	@Test // DATAREDIS-469
	void testGetAndSet() {

		intCounter.set(1);
		assertThat(intCounter.getAndSet(5)).isOne();
		assertThat(intCounter.get()).isEqualTo(5);
	}

	@Test // DATAREDIS-108, DATAREDIS-843
	void testCompareSet() throws Exception {

		AtomicBoolean alreadySet = new AtomicBoolean(false);
		int NUM = 50;
		String KEY = getClass().getSimpleName() + ":atomic:counter";
		CountDownLatch latch = new CountDownLatch(NUM);
		AtomicBoolean failed = new AtomicBoolean(false);
		RedisAtomicInteger atomicInteger = new RedisAtomicInteger(KEY, factory);

		for (int i = 0; i < NUM; i++) {

			new Thread(() -> {

				try {
					if (atomicInteger.compareAndSet(0, 1)) {
						if (alreadySet.get()) {
							failed.set(true);
						}
						alreadySet.set(true);
					}
				} finally {
					latch.countDown();
				}
			}).start();
		}

		latch.await();

		assertThat(failed.get()).withFailMessage("counter already modified").isFalse();
	}

	@Test // DATAREDIS-317
	void testShouldThrowExceptionIfRedisAtomicIntegerIsUsedWithRedisTemplateAndNoKeySerializer() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new RedisAtomicInteger("foo", new RedisTemplate<>()))
				.withMessageContaining("a valid key serializer in template is required");
	}

	@Test // DATAREDIS-317
	void testShouldThrowExceptionIfRedisAtomicIntegerIsUsedWithRedisTemplateAndNoValueSerializer() {

		RedisTemplate<String, Integer> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new RedisAtomicInteger("foo", template))
				.withMessageContaining("a valid value serializer in template is required");
	}

	@Test // DATAREDIS-317
	void testShouldBeAbleToUseRedisAtomicIntegerWithProperlyConfiguredRedisTemplate() {

		RedisAtomicInteger ral = new RedisAtomicInteger("DATAREDIS-317.atomicInteger", template);
		ral.set(32);

		assertThat(ral.get()).isEqualTo(32);
	}

	@Test // DATAREDIS-469
	void getThrowsExceptionWhenKeyHasBeenRemoved() {

		// setup integer
		RedisAtomicInteger test = new RedisAtomicInteger("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThatExceptionOfType(DataRetrievalFailureException.class).isThrownBy(test::get)
				.withMessageContaining("'test' seems to no longer exist");
	}

	@Test // DATAREDIS-469
	void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup integer
		RedisAtomicInteger test = new RedisAtomicInteger("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}

	@Test // DATAREDIS-874
	void updateAndGetAppliesGivenUpdateFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		int expectedNewValue = 10;
		intCounter.set(initialValue);

		IntUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		int result = intCounter.updateAndGet(updateFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(intCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void updateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		intCounter.set(initialValue);

		IntUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isEqualTo(initialValue);

			return -1;
		};

		intCounter.updateAndGet(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void getAndUpdateAppliesGivenUpdateFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		int expectedNewValue = 10;
		intCounter.set(initialValue);

		IntUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		int result = intCounter.getAndUpdate(updateFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(intCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void getAndUpdateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		intCounter.set(initialValue);

		IntUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isEqualTo(initialValue);

			return -1;
		};

		intCounter.getAndUpdate(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void accumulateAndGetAppliesGivenAccumulatorFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		int expectedNewValue = 10;
		intCounter.set(initialValue);

		IntBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		int result = intCounter.accumulateAndGet(15, accumulatorFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(intCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void accumulateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		intCounter.set(initialValue);

		IntBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isEqualTo(initialValue);
			assertThat(y).isEqualTo(15);

			return -1;
		};

		intCounter.accumulateAndGet(15, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void getAndAccumulateAppliesGivenAccumulatorFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean(false);
		int initialValue = 5;
		int expectedNewValue = 10;
		intCounter.set(initialValue);

		IntBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		int result = intCounter.getAndAccumulate(15, accumulatorFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(intCounter.get()).isEqualTo(expectedNewValue);
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@Test // DATAREDIS-874
	void getAndAccumulateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		int initialValue = 5;
		intCounter.set(initialValue);

		IntBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isEqualTo(initialValue);
			assertThat(y).isEqualTo(15);

			return -1;
		};

		intCounter.getAndAccumulate(15, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}
}
