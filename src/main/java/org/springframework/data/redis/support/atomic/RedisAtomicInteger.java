/*
 * Copyright 2011-2016 the original author or authors.
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
 * Atomic integer backed by Redis. Uses Redis atomic increment/decrement and watch/multi/exec operations for CAS
 * operations.
 *
 * @see java.util.concurrent.atomic.AtomicInteger
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RedisAtomicInteger extends Number implements Serializable, BoundKeyOperations<String> {

	private static final long serialVersionUID = 1L;

	private volatile String key;
	private ValueOperations<String, Integer> operations;
	private RedisOperations<String, Integer> generalOps;

	/**
	 * Constructs a new <code>RedisAtomicInteger</code> instance. Uses the value existing in Redis or 0 if none is found.
	 *
	 * @param redisCounter redis counter
	 * @param factory connection factory
	 */
	public RedisAtomicInteger(String redisCounter, RedisConnectionFactory factory) {
		this(redisCounter, factory, null);
	}

	/**
	 * Constructs a new <code>RedisAtomicInteger</code> instance.
	 *
	 * @param redisCounter the redis counter
	 * @param factory the factory
	 * @param initialValue the initial value
	 */
	public RedisAtomicInteger(String redisCounter, RedisConnectionFactory factory, int initialValue) {
		this(redisCounter, factory, Integer.valueOf(initialValue));
	}

	/**
	 * Constructs a new <code>RedisAtomicInteger</code> instance. Uses the value existing in Redis or 0 if none is found.
	 *
	 * @param redisCounter the redis counter
	 * @param template the template
	 * @see #RedisAtomicInteger(String, RedisConnectionFactory, int)
	 */
	public RedisAtomicInteger(String redisCounter, RedisOperations<String, Integer> template) {
		this(redisCounter, template, null);
	}

	/**
	 * Constructs a new <code>RedisAtomicInteger</code> instance. Note: You need to configure the given {@code template}
	 * with appropriate {@link RedisSerializer} for the key and value. As an alternative one could use the
	 * {@link #RedisAtomicInteger(String, RedisConnectionFactory, Integer)} constructor which uses appropriate default
	 * serializers.
	 *
	 * @param redisCounter the redis counter
	 * @param template the template
	 * @param initialValue the initial value
	 */
	public RedisAtomicInteger(String redisCounter, RedisOperations<String, Integer> template, int initialValue) {
		this(redisCounter, template, Integer.valueOf(initialValue));
	}

	private RedisAtomicInteger(String redisCounter, RedisConnectionFactory factory, Integer initialValue) {
		RedisTemplate<String, Integer> redisTemplate = new RedisTemplate<String, Integer>();
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setValueSerializer(new GenericToStringSerializer<Integer>(Integer.class));
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

	private RedisAtomicInteger(String redisCounter, RedisOperations<String, Integer> template, Integer initialValue) {

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
	 * Get the current value.
	 *
	 * @return the current value
	 */
	public int get() {

		Integer value = operations.get(key);
		if (value != null) {
			return value.intValue();
		}

		throw new DataRetrievalFailureException(String.format("The key '%s' seems to no longer exist.", key));
	}

	/**
	 * Set to the given value.
	 *
	 * @param newValue the new value
	 */
	public void set(int newValue) {
		operations.set(key, newValue);
	}

	/**
	 * Set to the give value and return the old value.
	 *
	 * @param newValue the new value
	 * @return the previous value
	 */
	public int getAndSet(int newValue) {

		Integer value = operations.getAndSet(key, newValue);
		if (value != null) {
			return value.intValue();
		}

		return 0;
	}

	/**
	 * Atomically set the value to the given updated value if the current value <tt>==</tt> the expected value.
	 *
	 * @param expect the expected value
	 * @param update the new value
	 * @return true if successful. False return indicates that the actual value was not equal to the expected value.
	 */
	public boolean compareAndSet(final int expect, final int update) {
		return generalOps.execute(new SessionCallback<Boolean>() {

			@SuppressWarnings("unchecked")
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
	 * Atomically increment by one the current value.
	 *
	 * @return the previous value
	 */
	public int getAndIncrement() {
		return incrementAndGet() - 1;
	}

	/**
	 * Atomically decrement by one the current value.
	 *
	 * @return the previous value
	 */
	public int getAndDecrement() {
		return decrementAndGet() + 1;
	}

	/**
	 * Atomically add the given value to current value.
	 *
	 * @param delta the value to add
	 * @return the previous value
	 */
	public int getAndAdd(final int delta) {
		return addAndGet(delta) - delta;
	}

	/**
	 * Atomically increment by one the current value.
	 *
	 * @return the updated value
	 */
	public int incrementAndGet() {
		return operations.increment(key, 1).intValue();
	}

	/**
	 * Atomically decrement by one the current value.
	 *
	 * @return the updated value
	 */
	public int decrementAndGet() {
		return operations.increment(key, -1).intValue();
	}

	/**
	 * Atomically add the given value to current value.
	 *
	 * @param delta the value to add
	 * @return the updated value
	 */
	public int addAndGet(int delta) {
		return operations.increment(key, delta).intValue();
	}

	/**
	 * Returns the String representation of the current value.
	 *
	 * @return the String representation of the current value.
	 */
	public String toString() {
		return Integer.toString(get());
	}

	public int intValue() {
		return get();
	}

	public long longValue() {
		return (long) get();
	}

	public float floatValue() {
		return (float) get();
	}

	public double doubleValue() {
		return (double) get();
	}

	public String getKey() {
		return key;
	}

	public Boolean expire(long timeout, TimeUnit unit) {
		return generalOps.expire(key, timeout, unit);
	}

	public Boolean expireAt(Date date) {
		return generalOps.expireAt(key, date);
	}

	public Long getExpire() {
		return generalOps.getExpire(key);
	}

	public Boolean persist() {
		return generalOps.persist(key);
	}

	public void rename(String newKey) {
		generalOps.rename(key, newKey);
		key = newKey;
	}

	public DataType getType() {
		return DataType.STRING;
	}
}
