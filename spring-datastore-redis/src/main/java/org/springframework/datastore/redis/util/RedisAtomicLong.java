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
package org.springframework.datastore.redis.util;

import java.io.Serializable;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Atomic long backed by Redis.
 * Uses Redis atomic increment/decrement and watch/multi/exec commands for CAS operations.
 *  
 * @see java.util.concurrent.atomic.AtomicLong
 * @author Costin Leau
 */
public class RedisAtomicLong extends Number implements Serializable {

	private final String key;
	private RedisCommands commands;

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance with an initial value of zero.
	 *
	 * @param redisCounter
	 * @param commands
	 */
	public RedisAtomicLong(String redisCounter, RedisCommands commands) {
		this(redisCounter, commands, 0);
	}

	/**
	 * Constructs a new <code>RedisAtomicLong</code> instance with the given initial value.
	 *
	 * @param redisCounter
	 * @param commands
	 * @param initialValue
	 */
	public RedisAtomicLong(String redisCounter, RedisCommands commands, long initialValue) {
		this.key = redisCounter;
		this.commands = commands;
		commands.set(redisCounter, Long.toString(initialValue));
	}

	/**
	 * Gets the current value.
	 *
	 * @return the current value
	 */
	public long get() {
		return Long.valueOf(commands.get(key));
	}

	/**
	 * Sets to the given value.
	 *
	 * @param newValue the new value
	 */
	public void set(long newValue) {
		commands.set(key, Long.toString(newValue));
	}

	/**
	 * Atomically sets to the given value and returns the old value.
	 *
	 * @param newValue the new value
	 * @return the previous value
	 */
	public long getAndSet(long newValue) {
		return Long.valueOf(commands.getSet(key, Long.toString(newValue)));
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
			commands.watch(key);
			if (expect == get()) {
				commands.multi();
				set(update);
				if (commands.exec() != null) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Atomically increments by one the current value.
	 *
	 * @return the previous value
	 */
	public long getAndIncrement() {
		for (;;) {
			commands.watch(key);
			long value = get();
			commands.multi();
			commands.incr(key);
			if (commands.exec() != null) {
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
			commands.watch(key);
			long value = get();
			commands.multi();
			commands.decr(key);
			if (commands.exec() != null) {
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
			commands.watch(key);
			long value = get();
			commands.multi();
			set(value + delta);
			if (commands.exec() != null) {
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
		return commands.incr(key);
	}

	/**
	 * Atomically decrements by one the current value.
	 *
	 * @return the updated value
	 */
	public long decrementAndGet() {
		return commands.decr(key);
	}

	/**
	 * Atomically adds the given value to the current value.
	 *
	 * @param delta the value to add
	 * @return the updated value
	 */
	public long addAndGet(long delta) {
		// TODO: is this really safe
		return commands.incrBy(key, (int) delta);
	}

	/**
	 * Returns the String representation of the current value.
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