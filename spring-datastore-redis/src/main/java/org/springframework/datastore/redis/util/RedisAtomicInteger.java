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
 * Atomic integer backed by Redis.
 * Uses Redis atomic increment/decrement and watch/multi/exec commands for CAS operations. 
 * 
 * @see java.util.concurrent.atomic.AtomicInteger
 * @author Costin Leau
 */
public class RedisAtomicInteger extends Number implements Serializable {

	private static final long serialVersionUID = 5984507176128031015L;

	private final String key;
	private RedisCommands commands;

	public RedisAtomicInteger(String redisCounter, RedisCommands commands) {
		this.key = redisCounter;
		this.commands = commands;
	}

	/**
	 * Get the current value.
	 *
	 * @return the current value
	 */
	public int get() {
		return Integer.valueOf(commands.get(key));
	}

	/**
	 * Set to the given value.
	 *
	 * @param newValue the new value
	 */
	public void set(int newValue) {
		commands.set(key, Integer.toString(newValue));
	}

	/**
	 * Set to the give value and return the old value.
	 *
	 * @param newValue the new value
	 * @return the previous value
	 */
	public int getAndSet(int newValue) {
		return Integer.valueOf(commands.getSet(key, Integer.toString(newValue)));
	}


	/**
	 * Atomically set the value to the given updated value
	 * if the current value <tt>==</tt> the expected value.
	 * @param expect the expected value
	 * @param update the new value
	 * @return true if successful. False return indicates that
	 * the actual value was not equal to the expected value.
	 */
	public boolean compareAndSet(int expect, int update) {
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
	 * Atomically increment by one the current value.
	 * @return the previous value
	 */
	public int getAndIncrement() {
		for (;;) {
			commands.watch(key);
			int value = get();
			commands.multi();
			commands.incr(key);
			if (commands.exec() != null) {
				return value;
			}
		}
	}


	/**
	 * Atomically decrement by one the current value.
	 * @return the previous value
	 */
	public int getAndDecrement() {
		for (;;) {
			commands.watch(key);
			int value = get();
			commands.multi();
			commands.decr(key);
			if (commands.exec() != null) {
				return value;
			}
		}
	}


	/**
	 * Atomically add the given value to current value.
	 * @param delta the value to add
	 * @return the previous value
	 */
	public int getAndAdd(int delta) {
		for (;;) {
			commands.watch(key);
			int value = get();
			commands.multi();
			set(value + delta);
			if (commands.exec() != null) {
				return value;
			}
		}
	}

	/**
	 * Atomically increment by one the current value.
	 * @return the updated value
	 */
	public int incrementAndGet() {
		return commands.incr(key);
	}

	/**
	 * Atomically decrement by one the current value.
	 * @return the updated value
	 */
	public int decrementAndGet() {
		return commands.decr(key);
	}


	/**
	 * Atomically add the given value to current value.
	 * @param delta the value to add
	 * @return the updated value
	 */
	public int addAndGet(int delta) {
		return commands.incrBy(key, delta);
	}

	/**
	 * Returns the String representation of the current value.
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
}