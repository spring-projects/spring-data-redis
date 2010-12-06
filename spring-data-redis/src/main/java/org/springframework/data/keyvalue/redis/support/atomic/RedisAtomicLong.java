/*
 * Copyright 2006-2009 the original author or authors.
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

import org.springframework.data.keyvalue.redis.core.RedisOperations;
import org.springframework.data.keyvalue.redis.core.ValueOperations;

/**
 * Atomic long backed by Redis.
 * Uses Redis atomic increment/decrement and watch/multi/exec operations for CAS operations.
 *  
 * @see java.util.concurrent.atomic.AtomicLong
 * @author Costin Leau
 */
public class RedisAtomicLong extends Number implements Serializable {

	private final String key;
	private ValueOperations<String, Long> operations;
	private RedisOperations<String, Long> generalOps;

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance with an initial value of zero.
	 *
	 * @param redisCounter
	 * @param operations
	 */
	public RedisAtomicLong(String redisCounter, RedisOperations<String, Long> operations) {
		this(redisCounter, operations, 0);
	}

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance with the given initial value.
	 *
	 * @param redisCounter
	 * @param operations
	 * @param initialValue
	 */
	public RedisAtomicLong(String redisCounter, RedisOperations<String, Long> operations, long initialValue) {
		this.key = redisCounter;
		this.operations = operations.valueOps();
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
	public boolean compareAndSet(long expect, long update) {
		for (;;) {
			generalOps.watch(Collections.singleton(key));
			if (expect == get()) {
				generalOps.multi();
				set(update);
				if (generalOps.exec() != null) {
					return true;
				}
			}
			else {
				return false;
			}
		}
	}

	/**
	 * Atomically increments by one the current value.
	 *
	 * @return the previous value
	 */
	public long getAndIncrement() {
		for (;;) {
			generalOps.watch(Collections.singleton(key));
			long value = get();
			generalOps.multi();
			operations.increment(key, 1);
			if (generalOps.exec() != null) {
				return value;
			}
		}
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the previous value
	 */
	public long getAndDecrement() {
		for (;;) {
			generalOps.watch(Collections.singleton(key));
			long value = get();
			generalOps.multi();
			operations.increment(key, -1);
			if (generalOps.exec() != null) {
				return value;
			}
		}
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the previous value
	 */
	public long getAndAdd(long delta) {
		for (;;) {
			generalOps.watch(Collections.singleton(key));
			long value = get();
			generalOps.multi();
			set(value + delta);
			if (generalOps.exec() != null) {
				return value;
			}
		}
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
		// TODO: is this really safe
		return operations.increment(key, (int) delta);
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
		return (long) get();
	}

	public float floatValue() {
		return (float) get();
	}

	public double doubleValue() {
		return (double) get();
	}
}