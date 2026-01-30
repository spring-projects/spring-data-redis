/*
 * Copyright 2011-present the original author or authors.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * Value (or String in Redis terminology) operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Jiahe Cai
 * @author Christoph Strobl
 * @author Marcin Grzejszczak
 */
@NullUnmarked
public interface BoundValueOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Set {@code value} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(@NonNull V value);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	void set(@NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key. Return the old string stored at key, or
	 * {@literal null} if key did not exist. An error is returned and SET aborted if the value stored at key is not a
	 * string.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 3.5
	 */
	V setGet(@NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key. Return the old string stored at key, or
	 * {@literal null} if key did not exist. An error is returned and SET aborted if the value stored at key is not a
	 * string.
	 *
	 * @param value must not be {@literal null}.
	 * @param duration expiration duration
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 3.5
	 */
	V setGet(@NonNull V value, @NonNull Duration duration);

	/**
	 * Set the {@code value} and expiration {@code timeout} for the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @throws IllegalArgumentException if either {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 * @since 2.1
	 */
	default void set(@NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

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
	Boolean setIfAbsent(@NonNull V value);

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
	Boolean setIfAbsent(@NonNull V value, long timeout, @NonNull TimeUnit unit);

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
	default Boolean setIfAbsent(@NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

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
	Boolean setIfPresent(@NonNull V value);

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
	Boolean setIfPresent(@NonNull V value, long timeout, @NonNull TimeUnit unit);

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
	default Boolean setIfPresent(@NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfPresent(value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfPresent(value, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set the bound key to hold the string {@code value}, if and only if the current value
	 * is equal to the {@code oldValue}.
	 *
	 * @param newValue must not be {@literal null}.
	 * @param compareValue must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 4.1
	 * @see <a href="https://redis.io/commands/setnx">Redis Documentation: SET</a>
	 */
	Boolean setIfEqual(@NonNull V newValue, @NonNull V compareValue);

	/**
	 * Set the bound key to hold the string {@code value} and expiration {@code timeout}, if and only if the current value
	 * is equal to the {@code oldValue}.
	 *
	 * @param newValue must not be {@literal null}.
	 * @param compareValue must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 4.1
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean setIfEqual(@NonNull V newValue, @NonNull V compareValue, @NonNull Expiration expiration);

	/**
	 * Set the bound key to hold the string {@code value}, if and only if the current value
	 * is not equal to the {@code oldValue}.
	 *
	 * @param newValue must not be {@literal null}.
	 * @param compareValue must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 4.1
	 * @see <a href="https://redis.io/commands/setnx">Redis Documentation: SET</a>
	 */
	Boolean setIfNotEqual(@NonNull V newValue, @NonNull V compareValue);

	/**
	 * Set the bound key to hold the string {@code value} and expiration {@code timeout}, if and only if the current value
	 * is not equal to the {@code oldValue}.
	 *
	 * @param newValue must not be {@literal null}.
	 * @param compareValue must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 4.1
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean setIfNotEqual(@NonNull V newValue, @NonNull V compareValue, @NonNull Expiration expiration);

	/**
	 * Get the value of the bound key.
	 *
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	V get();

	/**
	 * Return the value at the bound key and delete the key.
	 *
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Redis Documentation: GETDEL</a>
	 * @since 2.6
	 */
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
	V getAndExpire(long timeout, @NonNull TimeUnit unit);

	/**
	 * Return the value at the bound key and expire the key by applying {@code timeout}.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	V getAndExpire(@NonNull Duration timeout);

	/**
	 * Return the value at the bound key and persist the key. This operation removes any TTL that is associated with the
	 * bound key.
	 *
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	V getAndPersist();

	/**
	 * Set {@code value} of the bound key and return its old value.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	V getAndSet(@NonNull V value);

	/**
	 * Increment an integer value stored as string value under the bound key by one.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	Long increment();

	/**
	 * Increment an integer value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 */
	Long increment(long delta);

	/**
	 * Increment a floating point number value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	Double increment(double delta);

	/**
	 * Decrement an integer value stored as string value under the bound key by one.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decr">Redis Documentation: DECR</a>
	 */
	Long decrement();

	/**
	 * Decrement an integer value stored as string value under the bound key by {@code delta}.
	 *
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 */
	Long decrement(long delta);

	/**
	 * Append a {@code value} to the bound key.
	 *
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Integer append(@NonNull String value);

	/**
	 * Get a substring of value of the bound key between {@code begin} and {@code end}.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	String get(long start, long end);

	/**
	 * Overwrite parts of the bound key starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param value must not be {@literal null}.
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void set(@NonNull V value, long offset);

	/**
	 * Get the length of the value stored at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	Long size();

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();
}
