/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.data.redis.support.atomic;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.BoundKeyOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * Atomic double backed by Redis. Uses Redis atomic increment/decrement and watch/multi/exec operations for CAS
 * operations.
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RedisAtomicDouble extends Number implements Serializable, BoundKeyOperations<String> {

	private static final long serialVersionUID = 1L;

	private volatile String key;
	private ValueOperations<String, Double> operations;
	private RedisOperations<String, Double> generalOps;

	/**
	 * Constructs a new <code>RedisAtomicDouble</code> instance. Uses the value existing in Redis or 0 if none is found.
	 *
	 * @param redisCounter redis counter
	 * @param factory connection factory
	 */
	public RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory) {
		this(redisCounter, factory, null);
	}

	/**
	 * Constructs a new <code>RedisAtomicDouble</code> instance.
	 *
	 * @param redisCounter
	 * @param factory
	 * @param initialValue
	 */
	public RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory, double initialValue) {
		this(redisCounter, factory, Double.valueOf(initialValue));
	}

	private RedisAtomicDouble(String redisCounter, RedisConnectionFactory factory, Double initialValue) {
		Assert.hasText(redisCounter, "a valid counter name is required");
		Assert.notNull(factory, "a valid factory is required");

		RedisTemplate<String, Double> redisTemplate = new RedisTemplate<String, Double>();
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setValueSerializer(new GenericToStringSerializer<Double>(Double.class));
		redisTemplate.setExposeConnection(true);
		redisTemplate.setConnectionFactory(factory);
		redisTemplate.afterPropertiesSet();

		this.key = redisCounter;
		this.generalOps = redisTemplate;
		this.operations = generalOps.opsForValue();

		if (initialValue == null) {
			if (this.operations.get(redisCounter) == null) {
				set(0);
			}
		} else {
			set(initialValue);
		}
	}

	/**
	 * Constructs a new <code>RedisAtomicDouble</code> instance. Uses the value existing in Redis or 0 if none is found.
	 *
	 * @param redisCounter the redis counter
	 * @param template the template
	 * @see #RedisAtomicDouble(String, RedisConnectionFactory, double)
	 */
	public RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template) {
		this(redisCounter, template, null);
	}

	/**
	 * Constructs a new <code>RedisAtomicDouble</code> instance. Note: You need to configure the given {@code template}
	 * with appropriate {@link RedisSerializer} for the key and value. As an alternative one could use the
	 * {@link #RedisAtomicDouble(String, RedisConnectionFactory, Double)} constructor which uses appropriate default
	 * serializers.
	 *
	 * @param redisCounter the redis counter
	 * @param template the template
	 * @param initialValue the initial value
	 */
	public RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template, double initialValue) {
		this(redisCounter, template, Double.valueOf(initialValue));
	}

	private RedisAtomicDouble(String redisCounter, RedisOperations<String, Double> template, Double initialValue) {

		Assert.hasText(redisCounter, "a valid counter name is required");
		Assert.notNull(template, "a valid template is required");
		Assert.notNull(template.getKeySerializer(), "a valid key serializer in template is required");
		Assert.notNull(template.getValueSerializer(), "a valid value serializer in template is required");

		this.key = redisCounter;
		this.generalOps = template;
		this.operations = generalOps.opsForValue();

		if (initialValue == null) {
			if (this.operations.get(redisCounter) == null) {
				set(0);
			}
		} else {
			set(initialValue);
		}
	}

	/**
	 * Gets the current value.
	 *
	 * @return the current value
	 */
	public double get() {

		Double value = operations.get(key);
		if (value != null) {
			return value.doubleValue();
		}

		throw new DataRetrievalFailureException(String.format("The key '%s' seems to no longer exist.", key));
	}

	/**
	 * Sets to the given value.
	 *
	 * @param newValue the new value
	 */
	public void set(double newValue) {
		operations.set(key, newValue);
	}

	/**
	 * Atomically sets to the given value and returns the old value.
	 *
	 * @param newValue the new value
	 * @return the previous value
	 */
	public double getAndSet(double newValue) {

		Double value = operations.getAndSet(key, newValue);
		if (value != null) {
			return value.doubleValue();
		}

		return 0;
	}

	/**
	 * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
	 *
	 * @param expect the expected value
	 * @param update the new value
	 * @return true if successful. False return indicates that the actual value was not equal to the expected value.
	 */
	public boolean compareAndSet(final double expect, final double update) {
		return generalOps.execute(new SessionCallback<Boolean>() {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			public Boolean execute(RedisOperations operations) {
				for (;;) {
					operations.watch(Collections.singleton(key));
					if (expect == get()) {
						generalOps.multi();
						set(update);
						if (operations.exec() != null) {
							return true;
						}
					}
					{
						return false;
					}
				}
			}
		});
	}

	/**
	 * Atomically increments by one the current value.
	 *
	 * @return the previous value
	 */
	public double getAndIncrement() {
		return incrementAndGet() - 1.0;
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the previous value
	 */
	public double getAndDecrement() {
		return decrementAndGet() + 1.0;
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the previous value
	 */
	public double getAndAdd(final double delta) {
		return addAndGet(delta) - delta;
	}

	/**
	 * Atomically increments by one the current value.
	 *
	 * @return the updated value
	 */
	public double incrementAndGet() {
		return operations.increment(key, 1.0);
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the updated value
	 */
	public double decrementAndGet() {
		return operations.increment(key, -1.0);
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the updated value
	 */
	public double addAndGet(double delta) {
		return operations.increment(key, delta);
	}

	/**
	 * Returns the String representation of the current value.
	 *
	 * @return the String representation of the current value.
	 */
	public String toString() {
		return Double.toString(get());
	}

	public String getKey() {
		return key;
	}

	public DataType getType() {
		return DataType.STRING;
	}

	public Long getExpire() {
		return generalOps.getExpire(key);
	}

	public Boolean expire(long timeout, TimeUnit unit) {
		return generalOps.expire(key, timeout, unit);
	}

	public Boolean expireAt(Date date) {
		return generalOps.expireAt(key, date);
	}

	public Boolean persist() {
		return generalOps.persist(key);
	}

	public void rename(String newKey) {
		generalOps.rename(key, newKey);
		key = newKey;
	}

	@Override
	public double doubleValue() {
		return get();
	}

	@Override
	public float floatValue() {
		return (float) get();
	}

	@Override
	public int intValue() {
		return (int) get();
	}

	@Override
	public long longValue() {
		return (long) get();
	}
}
