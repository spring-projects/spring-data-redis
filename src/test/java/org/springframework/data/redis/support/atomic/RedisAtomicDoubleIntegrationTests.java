/*
 * Copyright 2013-2023 the original author or authors.
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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import org.assertj.core.data.Offset;
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
 * Integration test of {@link RedisAtomicDouble}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Graham MacMaster
 */
@MethodSource("testParams")
public class RedisAtomicDoubleIntegrationTests {

	private final RedisConnectionFactory factory;
	private final RedisTemplate<String, Double> template;

	private RedisAtomicDouble doubleCounter;

	public RedisAtomicDoubleIntegrationTests(RedisConnectionFactory factory) {

		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Double.class));
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

		this.doubleCounter = new RedisAtomicDouble(getClass().getSimpleName() + ":double", factory);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testCheckAndSet() {

		doubleCounter.set(0);
		assertThat(doubleCounter.compareAndSet(1.2, 10.6)).isFalse();
		assertThat(doubleCounter.compareAndSet(0, 10.6)).isTrue();
		assertThat(doubleCounter.compareAndSet(10.6, 0)).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testIncrementAndGet() {

		doubleCounter.set(0);
		assertThat(doubleCounter.incrementAndGet()).isEqualTo(1.0);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testAddAndGet() {

		doubleCounter.set(0);
		double delta = 1.3;
		assertThat(doubleCounter.addAndGet(delta)).isCloseTo(delta, Offset.offset(.0001));
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testDecrementAndGet() {

		doubleCounter.set(1);
		assertThat(doubleCounter.decrementAndGet()).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testGetAndSet() {

		doubleCounter.set(3.4);
		assertThat(doubleCounter.getAndSet(1.2)).isEqualTo(3.4);
		assertThat(doubleCounter.get()).isEqualTo(1.2);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testGetAndIncrement() {

		doubleCounter.set(2.3);
		assertThat(doubleCounter.getAndIncrement()).isEqualTo(2.3);
		assertThat(doubleCounter.get()).isEqualTo(3.3);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testGetAndDecrement() {

		doubleCounter.set(0.5);
		assertThat(doubleCounter.getAndDecrement()).isEqualTo(0.5);
		assertThat(doubleCounter.get()).isEqualTo(-0.5);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testGetAndAdd() {

		doubleCounter.set(0.5);
		assertThat(doubleCounter.getAndAdd(0.7)).isEqualTo(0.5);
		assertThat(doubleCounter.get()).isEqualTo(1.2);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testExpire() {

		assertThat(doubleCounter.expire(1, TimeUnit.SECONDS)).isTrue();
		assertThat(doubleCounter.getExpire()).isGreaterThan(0);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testExpireAt() {

		doubleCounter.set(7.8);
		assertThat(doubleCounter.expireAt(new Date(System.currentTimeMillis() + 10000))).isTrue();
		assertThat(doubleCounter.getExpire()).isGreaterThan(0);
	}

	@ParameterizedRedisTest // DATAREDIS-198
	void testRename() {

		doubleCounter.set(5.6);
		doubleCounter.rename("foodouble");
		assertThat(new String(factory.getConnection().get("foodouble".getBytes()))).isEqualTo("5.6");
		assertThat(factory.getConnection().get((getClass().getSimpleName() + ":double").getBytes())).isNull();
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldThrowExceptionIfRedisAtomicDoubleIsUsedWithRedisTemplateAndNoKeySerializer() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> new RedisAtomicDouble("foo", new RedisTemplate<>()))
				.withMessageContaining("a valid key serializer in template is required");
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldThrowExceptionIfRedisAtomicDoubleIsUsedWithRedisTemplateAndNoValueSerializer() {


		RedisTemplate<String, Double> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new RedisAtomicDouble("foo", template))
				.withMessageContaining("a valid value serializer in template is required");
	}

	@ParameterizedRedisTest // DATAREDIS-317
	void testShouldBeAbleToUseRedisAtomicDoubleWithProperlyConfiguredRedisTemplate() {

		RedisAtomicDouble ral = new RedisAtomicDouble("DATAREDIS-317.atomicDouble", template);
		ral.set(32.23);

		assertThat(ral.get()).isEqualTo(32.23);
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void getThrowsExceptionWhenKeyHasBeenRemoved() {

		// setup double
		RedisAtomicDouble test = new RedisAtomicDouble("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThatExceptionOfType(DataRetrievalFailureException.class).isThrownBy(() -> test.get())
				.withMessageContaining("'test' seems to no longer exist");
	}

	@ParameterizedRedisTest // DATAREDIS-469
	void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup double
		RedisAtomicDouble test = new RedisAtomicDouble("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void updateAndGetAppliesGivenUpdateFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		double expectedNewValue = 10.6;
		doubleCounter.set(initialValue);

		DoubleUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		double result = doubleCounter.updateAndGet(updateFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(doubleCounter.get()).isCloseTo(expectedNewValue, Offset.offset(0.001));
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void updateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		doubleCounter.set(initialValue);

		DoubleUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isCloseTo(initialValue, Offset.offset(0.001));

			return -1;
		};

		doubleCounter.updateAndGet(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndUpdateAppliesGivenUpdateFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		double expectedNewValue = 10.6;
		doubleCounter.set(initialValue);

		DoubleUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		double result = doubleCounter.getAndUpdate(updateFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(doubleCounter.get()).isCloseTo(expectedNewValue, Offset.offset(0.001));
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndUpdateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		doubleCounter.set(initialValue);

		DoubleUnaryOperator updateFunction = input -> {

			operatorHasBeenApplied.set(true);

			assertThat(input).isCloseTo(initialValue, Offset.offset(0.001));

			return -1;
		};

		doubleCounter.getAndUpdate(updateFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void accumulateAndGetAppliesGivenAccumulatorFunctionAndReturnsUpdatedValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		double expectedNewValue = 10.6;
		doubleCounter.set(initialValue);

		DoubleBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		double result = doubleCounter.accumulateAndGet(15.9, accumulatorFunction);

		assertThat(result).isEqualTo(expectedNewValue);
		assertThat(doubleCounter.get()).isCloseTo(expectedNewValue, Offset.offset(0.001));
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void accumulateAndGetUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		doubleCounter.set(initialValue);

		DoubleBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isCloseTo(initialValue, Offset.offset(0.001));
			assertThat(y).isCloseTo(15, Offset.offset(0.001));

			return -1;
		};

		doubleCounter.accumulateAndGet(15, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndAccumulateAppliesGivenAccumulatorFunctionAndReturnsOriginalValue() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		double expectedNewValue = 10.6;
		doubleCounter.set(initialValue);

		DoubleBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			return expectedNewValue;
		};

		double result = doubleCounter.getAndAccumulate(15.9, accumulatorFunction);

		assertThat(result).isEqualTo(initialValue);
		assertThat(doubleCounter.get()).isCloseTo(expectedNewValue, Offset.offset(0.001));
		assertThat(operatorHasBeenApplied).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-874
	void getAndAccumulateUsesCorrectArguments() {

		AtomicBoolean operatorHasBeenApplied = new AtomicBoolean();
		double initialValue = 5.3;
		doubleCounter.set(initialValue);

		DoubleBinaryOperator accumulatorFunction = (x, y) -> {

			operatorHasBeenApplied.set(true);

			assertThat(x).isCloseTo(initialValue, Offset.offset(0.001));
			assertThat(y).isCloseTo(15, Offset.offset(0.001));

			return -1;
		};

		doubleCounter.getAndAccumulate(15, accumulatorFunction);

		assertThat(operatorHasBeenApplied).isTrue();
	}
}
