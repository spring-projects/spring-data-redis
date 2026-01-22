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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.util.Assert;

/**
 * Redis operations for simple (or in Redis terminology 'string') values.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jiahe Cai
 * @author Marcin Grzejszczak
 * @author Chris Bono
 */
@NullUnmarked
public interface ValueOperations<K, V> {

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(@NonNull K key, @NonNull V value);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}. Return the old string stored at key, or
	 * {@literal null} if key did not exist. An error is returned and SET aborted if the value stored at key is not a
	 * string.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 3.5
	 */
	@Nullable
	V setGet(@NonNull K key, @NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}. Return the old string stored at key, or
	 * {@literal null} if key did not exist. An error is returned and SET aborted if the value stored at key is not a
	 * string.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param duration expiration duration
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 3.5
	 */
	@Nullable
	V setGet(@NonNull K key, @NonNull V value, @NonNull Duration duration);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	void set(@NonNull K key, @NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @throws IllegalArgumentException if either {@code key}, {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	default void set(@NonNull K key, @NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		if (TimeoutUtils.hasMillis(timeout)) {
			set(key, value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		} else {
			set(key, value, timeout.getSeconds(), TimeUnit.SECONDS);
		}
	}

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is absent.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean setIfAbsent(@NonNull K key, @NonNull V value);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is absent.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean setIfAbsent(@NonNull K key, @NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is absent.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if either {@code key}, {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	default Boolean setIfAbsent(@NonNull K key, @NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfAbsent(key, value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfAbsent(key, value, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is present.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if either {@code key} or {@code value} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	Boolean setIfPresent(@NonNull K key, @NonNull V value);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is present.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if either {@code key}, {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	Boolean setIfPresent(@NonNull K key, @NonNull V value, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is present.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if either {@code key}, {@code value} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 2.1
	 */
	default Boolean setIfPresent(@NonNull K key, @NonNull V value, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfPresent(key, value, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfPresent(key, value, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is present
	 * and the current value is equal to the {@code oldValue}.
	 *
	 * @param key   must not be {@literal null}.
	 * @param newValue must not be {@literal null}.
	 * @param oldValue must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if either {@code key}, {@code value} or {@code oldValue} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 4.1.0
	 */
	Boolean setIfEqual(@NonNull K key, @NonNull V newValue, @NonNull V oldValue);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is present
	 * and the current value is equal to the {@code oldValue}.
	 *
	 * @param key must not be {@literal null}.
	 * @param newValue must not be {@literal null}.
	 * @param oldValue must not be {@literal null}.
	 * @param timeout the key expiration timeout.
	 * @param unit must not be {@literal null}.
	 * @return command result indicating if the key has been set.
	 * @throws IllegalArgumentException if either {@code key}, {@code value}, {@code oldValue} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 4.1.0
	 */
	Boolean setIfEqual(@NonNull K key, @NonNull V newValue, @NonNull V oldValue, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set {@code key} to hold the string {@code value} and expiration {@code timeout} if {@code key} is present
	 * and the current value is equal to the {@code oldValue}.
	 *
	 * @param key must not be {@literal null}.
	 * @param newValue must not be {@literal null}.
	 * @param oldValue must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if either {@code key}, {@code value}, {@code oldValue} or {@code timeout} is not present.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @since 4.1.0
	 */
	default Boolean setIfEqual(@NonNull K key, @NonNull V newValue, @NonNull V oldValue, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		if (TimeoutUtils.hasMillis(timeout)) {
			return setIfEqual(key, newValue, oldValue, timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		return setIfEqual(key, newValue, oldValue, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param map must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	void multiSet(Map<? extends @NonNull K, ? extends @NonNull V> map);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param map must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/msetnx">Redis Documentation: MSETNX</a>
	 */
	Boolean multiSetIfAbsent(Map<? extends @NonNull K, ? extends @NonNull V> map);

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	@Nullable
	V get(Object key);

	/**
	 * Return the value at {@code key} and delete the key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Redis Documentation: GETDEL</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndDelete(@NonNull K key);

	/**
	 * Return the value at {@code key} and expire the key by applying {@code timeout}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndExpire(@NonNull K key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Return the value at {@code key} and expire the key by applying {@code timeout}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndExpire(@NonNull K key, @NonNull Duration timeout);

	/**
	 * Return the value at {@code key} and persist the key. This operation removes any TTL that is associated with
	 * {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	@Nullable
	V getAndPersist(@NonNull K key);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	@Nullable
	V getAndSet(@NonNull K key, @NonNull V value);

	/**
	 * Get multiple {@code keys}. Values are in the order of the requested keys Absent field values are represented using
	 * {@literal null} in the resulting {@link List}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/mget">Redis Documentation: MGET</a>
	 */
	List<@Nullable V> multiGet(@NonNull Collection<@NonNull K> keys);

	/**
	 * Increment an integer value stored as string value under {@code key} by one.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	Long increment(@NonNull K key);

	/**
	 * Increment an integer value stored as string value under {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 */
	Long increment(@NonNull K key, long delta);

	/**
	 * Increment a floating point number value stored as string value under {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	Double increment(@NonNull K key, double delta);

	/**
	 * Decrement an integer value stored as string value under {@code key} by one.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decr">Redis Documentation: DECR</a>
	 */
	Long decrement(@NonNull K key);

	/**
	 * Decrement an integer value stored as string value under {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 */
	Long decrement(@NonNull K key, long delta);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Integer append(@NonNull K key, @NonNull String value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	String get(@NonNull K key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void set(@NonNull K key, @NonNull V value, long offset);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	Long size(@NonNull K key);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 */
	Boolean setBit(@NonNull K key, long offset, boolean value);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/getbit">Redis Documentation: GETBIT</a>
	 */
	Boolean getBit(@NonNull K key, long offset);

	/**
	 * Get / Manipulate specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset stored
	 * at a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param subCommands must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/bitfield">Redis Documentation: BITFIELD</a>
	 */
	List<Long> bitField(@NonNull K key, @NonNull BitFieldSubCommands subCommands);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();

}
