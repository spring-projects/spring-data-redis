/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.support.atomic;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.BoundKeyOperations;
import org.springframework.data.keyvalue.redis.core.RedisOperations;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;
import org.springframework.data.keyvalue.redis.core.SessionCallback;
import org.springframework.data.keyvalue.redis.core.ValueOperations;
import org.springframework.data.keyvalue.redis.serializer.GenericToStringSerializer;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;

/**
 * Atomic long backed by Redis.
 * Uses Redis atomic increment/decrement and watch/multi/exec operations for CAS operations.
 *  
 * @see java.util.concurrent.atomic.AtomicLong
 * @author Costin Leau
 */
public class RedisAtomicLong extends Number implements Serializable, BoundKeyOperations<String> {

	private volatile String key;
	private ValueOperations<String, Long> operations;
	private RedisOperations<String, Long> generalOps;


	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance.
	 *
	 * @param redisCounter redis counter
	 * @param factory connection factory
	 */
	public RedisAtomicLong(String redisCounter, RedisConnectionFactory factory) {
		this(redisCounter, factory, null);
	}

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance.
	 *
	 * @param redisCounter
	 * @param factory
	 * @param initialValue
	 */
	public RedisAtomicLong(String redisCounter, RedisConnectionFactory factory, long initialValue) {
		this(redisCounter, factory, Long.valueOf(initialValue));
	}

	private RedisAtomicLong(String redisCounter, RedisConnectionFactory factory, Long initialValue) {
		RedisTemplate<String, Long> redisTemplate = new RedisTemplate<String, Long>();
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
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
		}
		else {
			set(initialValue);
		}
	}

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance. Uses as initial value
	 * the data from the backing store (sets the counter to 0 if no value is found).
	 *
	 * Use {@link #RedisAtomicLong(String, RedisOperations, long)} to set the counter to a certain value
	 * as an alternative constructor or {@link #set(long)}. 
	 * 
	 * Note that longs need to be properly serialized so that Redis can recognized the values as numeric and thus modify their value.
	 * 
	 * @param redisCounter
	 * @param operations
	 */
	public RedisAtomicLong(String redisCounter, RedisOperations<String, Long> operations) {
		this.key = redisCounter;
		this.operations = operations.opsForValue();
		this.generalOps = operations;
		if (this.operations.get(redisCounter) == null) {
			set(0);
		}
	}

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance with the given initial value.
	 *
	 * Note that longs need to be properly serialized so that Redis can recognized the values as numeric and thus modify their value.
	 * 
	 * @param redisCounter
	 * @param operations
	 * @param initialValue
	 */
	public RedisAtomicLong(String redisCounter, RedisOperations<String, Long> operations, long initialValue) {
		this.key = redisCounter;
		this.operations = operations.opsForValue();
		this.operations.set(redisCounter, initialValue);
	}

	/**
	 * Gets the current value.
	 *
	 * @return the current value
	 */
	public long get() {
		return operations.get(key);
	}

	/**
	 * Sets to the given value.
	 *
	 * @param newValue the new value
	 */
	public void set(long newValue) {
		operations.set(key, newValue);
	}

	/**
	 * Atomically sets to the given value and returns the old value.
	 *
	 * @param newValue the new value
	 * @return the previous value
	 */
	public long getAndSet(long newValue) {
		return operations.getAndSet(key, newValue);
	}

	/**
	 * Atomically sets the value to the given updated value
	 * if the current value {@code ==} the expected value.
	 *
	 * @param expect the expected value
	 * @param update the new value
	 * @return true if successful. False return indicates that
	 * the actual value was not equal to the expected value.
	 */
	public boolean compareAndSet(final long expect, final long update) {
		return generalOps.execute(new SessionCallback<Boolean>() {

			@SuppressWarnings("unchecked")
			@Override
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
	public long getAndIncrement() {
		return incrementAndGet() - 1;
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the previous value
	 */
	public long getAndDecrement() {
		return decrementAndGet() + 1;
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the previous value
	 */
	public long getAndAdd(final long delta) {
		return addAndGet(delta) - delta;
	}

	/**
	 * Atomically increments by one the current value.
	 *
	 * @return the updated value
	 */
	public long incrementAndGet() {
		return operations.increment(key, 1);
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the updated value
	 */
	public long decrementAndGet() {
		return operations.increment(key, -1);
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the updated value
	 */
	public long addAndGet(long delta) {
		return operations.increment(key, delta);
	}

	/**
	 * Returns the String representation of the current value.
	 * 
	 * @return the String representation of the current value.
	 */
	public String toString() {
		return Long.toString(get());
	}


	public int intValue() {
		return (int) get();
	}

	public long longValue() {
		return get();
	}

	public float floatValue() {
		return (float) get();
	}

	public double doubleValue() {
		return (double) get();
	}

	@Override
	public String getKey() {
		return key;
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
	public Long getExpire() {
		return generalOps.getExpire(key);
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
	public DataType getType() {
		return DataType.STRING;
	}
}