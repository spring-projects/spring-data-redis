/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Value (or String in Redis terminology) operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Jiahe Cai
 * @author Christoph Strobl
 */
public interface BoundValueOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Set {@code value} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(V value);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	void set(V value, long timeout, TimeUnit unit);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @throws IllegalArgumentException if either {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 * @since 2.1
	 */
	default void set(V value, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null!");

		if (TimeoutUtils.hasMillis(timeout)) {
			set(value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		} else {
			set(value, timeout.getSeconds(), TimeUnit.SECONDS);
		}
	}

	/**
	 * Set the bound key to hold the string {@code value} if the bound key is absent.
	 *
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 */
	@Nullable
	Boolean setIfAbsent(V value);

	/**
	 * Set the bound key to hold the string {@code value} and expiration {@code timeout} if the bound key is absent.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	@Nullable
	Boolean setIfAbsent(V value, long timeout, TimeUnit unit);

	/**
	 * Set bound key to hold the string {@code value} and expiration {@code timeout} if the bound key is absent.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if either {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	@Nullable
	default Boolean setIfAbsent(V value, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null!");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfAbsent(value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfAbsent(value, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set the bound key to hold the string {@code value} if the bound key is present.
	 *
	 * @param value must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if {@code value} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	@Nullable
	Boolean setIfPresent(V value);

	/**
	 * Set the bound key to hold the string {@code value} and expiration {@code timeout} if the bound key is present.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if either {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	@Nullable
	Boolean setIfPresent(V value, long timeout, TimeUnit unit);

	/**
	 * Set the bound key to hold the string {@code value} and expiration {@code timeout} if the bound key is present.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if either {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	@Nullable
	default Boolean setIfPresent(V value, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null!");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfPresent(value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfPresent(value, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Get the value of the bound key.
	 *
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	@Nullable
	V get();

	/**
	 * Return the value at the bound key and delete the key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Redis Documentation: GETDEL</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndDelete();

	/**
	 * Return the value at the bound key and expire the key by applying {@code timeout}.
	 *
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndExpire(long timeout, TimeUnit unit);

	/**
	 * Return the value at the bound key and expire the key by applying {@code timeout}.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndExpire(Duration timeout);

	/**
	 * Return the value at the bound key and persist the key. This operation removes any TTL that is associated with the
	 * bound key.
	 *
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndPersist();

	/**
	 * Set {@code value} of the bound key and return its old value.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	@Nullable
	V getAndSet(V value);

	/**
	 * Increment an integer value stored as string value under the bound key by one.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	@Nullable
	Long increment();

	/**
	 * Increment an integer value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 */
	@Nullable
	Long increment(long delta);

	/**
	 * Increment a floating point number value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	@Nullable
	Double increment(double delta);

	/**
	 * Decrement an integer value stored as string value under the bound key by one.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decr">Redis Documentation: DECR</a>
	 */
	@Nullable
	Long decrement();

	/**
	 * Decrement an integer value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 */
	@Nullable
	Long decrement(long delta);

	/**
	 * Append a {@code value} to the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	@Nullable
	Integer append(String value);

	/**
	 * Get a substring of value of the bound key between {@code begin} and {@code end}.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	@Nullable
	String get(long start, long end);

	/**
	 * Overwrite parts of the bound key starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param value must not be {@literal null}.
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void set(V value, long offset);

	/**
	 * Get the length of the value stored at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	@Nullable
	Long size();

	/**
	 * @return never {@literal null}.
	 */
	RedisOperations<K, V> getOperations();
}
