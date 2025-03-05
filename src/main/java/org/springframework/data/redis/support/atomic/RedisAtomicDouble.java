/*
 * Copyright 2013-2025 the original author or authors.
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

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.BoundKeyOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Atomic double backed by Redis. Uses Redis atomic increment/decrement and watch/multi/exec operations for CAS
 * operations.
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Graham MacMaster
 * @author Ning Wei
 */
public class RedisAtomicDouble extends Number implements Serializable, BoundKeyOperations<String> {

	@Serial
	private static final long serialVersionUID = 1L;

	private volatile String key;

	private final ValueOperations<String, Double> operations;
	private final RedisOperations<String, Double> generalOps;

	/**
	 * Constructs a new {@link RedisAtomicDouble} instance. Uses the value existing in Redis or {@code 0} if none is
	 * found.
	 *
	 * @param redisCounter Redis key of this counter.
	 * @param factory connection factory.
	 */
	public RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory) {
		this(redisCounter, factory, null);
	}

	/**
	 * Constructs a new {@link RedisAtomicDouble} instance with a {@code initialValue} that overwrites the existing value.
	 *
	 * @param redisCounter Redis key of this counter.
	 * @param factory connection factory.
	 * @param initialValue initial value to set.
	 */
	public RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory, double initialValue) {
		this(redisCounter, factory, Double.valueOf(initialValue));
	}

	private RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory, @Nullable Double initialValue) {

		Assert.hasText(redisCounter, "a valid counter name is required");
		Assert.notNull(factory, "a valid factory is required");

		RedisTemplate<String, Double> redisTemplate = new RedisTemplate<>();
		redisTemplate.setKeySerializer(RedisSerializer.string());
		redisTemplate.setValueSerializer(new GenericToStringSerializer<>(Double.class));
		redisTemplate.setExposeConnection(true);
		redisTemplate.setConnectionFactory(factory);
		redisTemplate.afterPropertiesSet();

		this.key = redisCounter;
		this.generalOps = redisTemplate;
		this.operations = generalOps.opsForValue();

		if (initialValue == null) {
			initializeIfAbsent();
		} else {
			set(initialValue);
		}
	}

	/**
	 * Constructs a new {@link RedisAtomicDouble} instance. Uses the value existing in Redis or 0 if none is found.
	 *
	 * @param redisCounter Redis key of this counter.
	 * @param template the template.
	 * @see #RedisAtomicDouble(String, RedisConnectionFactory, double)
	 */
	public RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template) {
		this(redisCounter, template, null);
	}

	/**
	 * Constructs a new {@link RedisAtomicDouble} instance with a {@code initialValue} that overwrites the existing value
	 * at {@code redisCounter}.
	 * <p>
	 * Note: You need to configure the given {@code template} with appropriate {@link RedisSerializer} for the key and
	 * value.
	 * <p>
	 * As an alternative one could use the {@link #RedisAtomicDouble(String, RedisConnectionFactory, Double)} constructor
	 * which uses appropriate default serializers.
	 *
	 * @param redisCounter Redis key of this counter.
	 * @param template the template
	 * @param initialValue initial value to set if the Redis key is absent.
	 */
	public RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template, double initialValue) {
		this(redisCounter, template, Double.valueOf(initialValue));
	}

	private RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template,
			@Nullable Double initialValue) {

		Assert.hasText(redisCounter, "a valid counter name is required");
		Assert.notNull(template, "a valid template is required");
		Assert.notNull(template.getKeySerializer(), "a valid key serializer in template is required");
		Assert.notNull(template.getValueSerializer(), "a valid value serializer in template is required");

		this.key = redisCounter;
		this.generalOps = template;
		this.operations = generalOps.opsForValue();

		if (initialValue == null) {
			initializeIfAbsent();
		} else {
			set(initialValue);
		}
	}

	private void initializeIfAbsent() {
		operations.setIfAbsent(key, (double) 0);
	}

	/**
	 * Get the current value.
	 *
	 * @return the current value.
	 */
	public double get() {

		Double value = operations.get(key);

		if (value != null) {
			return value;
		}

		throw new DataRetrievalFailureException("The key '%s' seems to no longer exist".formatted(key));
	}

	/**
	 * Set to the given value.
	 *
	 * @param newValue the new value.
	 */
	public void set(double newValue) {
		operations.set(key, newValue);
	}

	/**
	 * Set to the given value and return the old value.
	 *
	 * @param newValue the new value.
	 * @return the previous value.
	 */
	public double getAndSet(double newValue) {

		Double value = operations.getAndSet(key, newValue);

		return value != null ? value : 0;
	}

	/**
	 * Atomically set the value to the given updated value if the current value {@code ==} the expected value.
	 *
	 * @param expect the expected value.
	 * @param update the new value.
	 * @return {@literal true} if successful. {@literal false} indicates that the actual value was not equal to the
	 *         expected value.
	 */
	public boolean compareAndSet(double expect, double update) {
		return generalOps.execute(new CompareAndSet<>(this::get, this::set, key, expect, update));
	}

	/**
	 * Atomically increment by one the current value.
	 *
	 * @return the previous value.
	 */
	public double getAndIncrement() {
		return incrementAndGet() - 1.0;
	}

	/**
	 * Atomically decrement by one the current value.
	 *
	 * @return the previous value.
	 */
	public double getAndDecrement() {
		return decrementAndGet() + 1.0;
	}

	/**
	 * Atomically add the given value to current value.
	 *
	 * @param delta the value to add.
	 * @return the previous value.
	 */
	public double getAndAdd(double delta) {
		return addAndGet(delta) - delta;
	}

	/**
	 * Atomically update the current value using the given {@link DoubleUnaryOperator update function}.
	 *
	 * @param updateFunction the function which calculates the value to set. Should be a pure function (no side effects),
	 *          because it will be applied several times if update attempts fail due to concurrent calls. Must not be
	 *          {@literal null}.
	 * @return the previous value.
	 * @since 2.2
	 */
	public double getAndUpdate(DoubleUnaryOperator updateFunction) {

		Assert.notNull(updateFunction, "Update function must not be null");

		double previousValue, newValue;

		do {
			previousValue = get();
			newValue = updateFunction.applyAsDouble(previousValue);
		} while (!compareAndSet(previousValue, newValue));

		return previousValue;
	}

	/**
	 * Atomically update the current value using the given {@link DoubleBinaryOperator accumulator function}. The new
	 * value is calculated by applying the accumulator function to the current value and the given {@code updateValue}.
	 *
	 * @param updateValue the value which will be passed into the accumulator function.
	 * @param accumulatorFunction the function which calculates the value to set. Should be a pure function (no side
	 *          effects), because it will be applied several times if update attempts fail due to concurrent calls. Must
	 *          not be {@literal null}.
	 * @return the previous value.
	 * @since 2.2
	 */
	public double getAndAccumulate(double updateValue, DoubleBinaryOperator accumulatorFunction) {

		Assert.notNull(accumulatorFunction, "Accumulator function must not be null");

		double previousValue, newValue;

		do {
			previousValue = get();
			newValue = accumulatorFunction.applyAsDouble(previousValue, updateValue);
		} while (!compareAndSet(previousValue, newValue));

		return previousValue;
	}

	/**
	 * Atomically increment by one the current value.
	 *
	 * @return the updated value.
	 */
	public double incrementAndGet() {
		return operations.increment(key, 1.0);
	}

	/**
	 * Atomically decrement by one the current value.
	 *
	 * @return the updated value.
	 */
	public double decrementAndGet() {
		return operations.increment(key, -1.0);
	}

	/**
	 * Atomically add the given value to current value.
	 *
	 * @param delta the value to add.
	 * @return the updated value.
	 */
	public double addAndGet(double delta) {
		return operations.increment(key, delta);
	}

	/**
	 * Atomically update the current value using the given {@link DoubleUnaryOperator update function}.
	 *
	 * @param updateFunction the function which calculates the value to set. Should be a pure function (no side effects),
	 *          because it will be applied several times if update attempts fail due to concurrent calls. Must not be
	 *          {@literal null}.
	 * @return the updated value.
	 * @since 2.2
	 */
	public double updateAndGet(DoubleUnaryOperator updateFunction) {

		Assert.notNull(updateFunction, "Update function must not be null");

		double previousValue, newValue;

		do {
			previousValue = get();
			newValue = updateFunction.applyAsDouble(previousValue);
		} while (!compareAndSet(previousValue, newValue));

		return newValue;
	}

	/**
	 * Atomically update the current value using the given {@link DoubleBinaryOperator accumulator function}. The new
	 * value is calculated by applying the accumulator function to the current value and the given {@code updateValue}.
	 *
	 * @param updateValue the value which will be passed into the accumulator function.
	 * @param accumulatorFunction the function which calculates the value to set. Should be a pure function (no side
	 *          effects), because it will be applied several times if update attempts fail due to concurrent calls. Must
	 *          not be {@literal null}.
	 * @return the updated value.
	 * @since 2.2
	 */
	public double accumulateAndGet(double updateValue, DoubleBinaryOperator accumulatorFunction) {

		Assert.notNull(accumulatorFunction, "Accumulator function must not be null");

		double previousValue, newValue;

		do {
			previousValue = get();
			newValue = accumulatorFunction.applyAsDouble(previousValue, updateValue);
		} while (!compareAndSet(previousValue, newValue));

		return newValue;
	}

	/**
	 * @return the String representation of the current value.
	 */
	@Override
	public String toString() {
		return Double.toString(get());
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public DataType getType() {
		return DataType.STRING;
	}

	@Override
	public Long getExpire() {
		return generalOps.getExpire(key);
	}

	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return generalOps.expire(key, timeout, unit);
	}

	@Override
	public Boolean expireAt(Date date) {
		return generalOps.expireAt(key, date);
	}

	@Override
	public Boolean persist() {
		return generalOps.persist(key);
	}

	@Override
	public void rename(String newKey) {

		generalOps.rename(key, newKey);
		key = newKey;
	}

	@Override
	public RedisOperations<String, ?> getOperations() {
		return generalOps;
	}

	@Override
	public int intValue() {
		return (int) get();
	}

	@Override
	public long longValue() {
		return (long) get();
	}

	@Override
	public float floatValue() {
		return (float) get();
	}

	@Override
	public double doubleValue() {
		return get();
	}

}
