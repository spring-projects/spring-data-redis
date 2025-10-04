/*
 * Copyright 2011-2025 the original author or authors.
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
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;

/**
 * Redis map specific operations working on a hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Tihomir Mateev
 * @author Viktoriya Kutsarova
 */
@NullUnmarked
public interface HashOperations<H, HK, HV> {

	/**
	 * Delete given hash {@code hashKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long delete(@NonNull H key, @NonNull Object @NonNull... hashKeys);

	/**
	 * Determine if given hash {@code hashKey} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean hasKey(@NonNull H key, @NonNull Object hashKey);

	/**
	 * Get value for given {@code hashKey} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when key or hashKey does not exist or used in pipeline / transaction.
	 */
	HV get(@NonNull H key, @NonNull Object hashKey);

	/**
	 * Get values for given {@code hashKeys} from hash at {@code key}. Values are in the order of the requested keys
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> multiGet(@NonNull H key, @NonNull Collection<@NonNull HK> hashKeys);

    /**
     * Get and remove the value for given {@code hashKeys} from hash at {@code key}. Values are in the order of the
     * requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     *
     * @param key must not be {@literal null}.
     * @param hashKeys must not be {@literal null}.
	 * @return list of values for the given fields or {@literal null} when used in pipeline / transaction.
     * @since 4.0
     */
    List<HV> getAndDelete(@NonNull H key, @NonNull Collection<@NonNull HK> hashKeys);

    /**
     * Get and optionally expire the value for given {@code hashKeys} from hash at {@code key}. Values are in the order of
     * the requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     *
     * @param key must not be {@literal null}.
     * @param expiration is optional.
     * @param hashKeys must not be {@literal null}.
     * @return list of values for the given fields or {@literal null} when used in pipeline / transaction.
     * @since 4.0
     */
    List<HV> getAndExpire(@NonNull H key, Expiration expiration, @NonNull Collection<@NonNull HK> hashKeys);

    /**
     * Set multiple hash fields to multiple values using data provided in {@code m} with optional condition and expiration.
     *
     * @param key must not be {@literal null}.
     * @param m must not be {@literal null}.
     * @param condition is optional.
     * @param expiration is optional.
     * @return whether all fields were set or {@literal null} when used in pipeline / transaction.
     * @since 4.0
     */
    Boolean putAndExpire(@NonNull H key, @NonNull Map<? extends @NonNull HK, ? extends HV> m,
                         RedisHashCommands.HashFieldSetOption condition, Expiration expiration);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long increment(@NonNull H key, @NonNull HK hashKey, long delta);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Double increment(@NonNull H key, @NonNull HK hashKey, double delta);

	/**
	 * Return a random hash key from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	HK randomKey(@NonNull H key);

	/**
	 * Return a random entry from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map. Entry<@NonNull HK, HV> randomEntry(@NonNull H key);

	/**
	 * Return random hash keys from the hash stored at {@code key}. If the provided {@code count} argument is positive,
	 * return a list of distinct hash keys, capped either at {@code count} or the hash size. If {@code count} is negative,
	 * the behavior changes and the command is allowed to return the same hash key multiple times. In this case, the
	 * number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	List<@NonNull HK> randomKeys(@NonNull H key, long count);

	/**
	 * Return a random entries from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return. Must be positive.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map<@NonNull HK, HV> randomEntries(@NonNull H key, long count);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Set<@NonNull HK> keys(@NonNull H key);

	/**
	 * Returns the length of the value associated with {@code hashKey}. If either the {@code key} or the {@code hashKey}
	 * do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	Long lengthOfValue(@NonNull H key, @NonNull HK hashKey);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long size(@NonNull H key);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m}.
	 *
	 * @param key must not be {@literal null}.
	 * @param m must not be {@literal null}.
	 */
	void putAll(@NonNull H key, @NonNull Map<? extends @NonNull HK, ? extends HV> m);

	/**
	 * Set the {@code value} of a hash {@code hashKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 */
	void put(@NonNull H key, @NonNull HK hashKey, HV value);

	/**
	 * Set the {@code value} of a hash {@code hashKey} only if {@code hashKey} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean putIfAbsent(@NonNull H key, @NonNull HK hashKey, HV value);

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> values(@NonNull H key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Map<@NonNull HK, HV> entries(@NonNull H key);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param key must not be {@literal null}.
	 * @param options can be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	@NonNull
	Cursor<Map.Entry<@NonNull HK, HV>> scan(@NonNull H key, @Nullable ScanOptions options);

	/**
	 * Set time to live for given {@code hashKey} .
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return changes to the hash fields. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	ExpireChanges<@NonNull HK> expire(@NonNull H key, @NonNull Duration timeout,
			@NonNull Collection<@NonNull HK> hashKeys);

	/**
	 * Set the expiration for given {@code hashKeys} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return changes to the hash fields. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	ExpireChanges<@NonNull HK> expireAt(@NonNull H key, @NonNull Instant expireAt,
			@NonNull Collection<@NonNull HK> hashKeys);

	/**
	 * Apply the expiration for given {@code hashKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return changes to the hash fields. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIREAT</a>
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpireat/">Redis Documentation: HPEXPIREAT</a>
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	ExpireChanges<@NonNull HK> expire(@NonNull H key, @NonNull Expiration expiration, @NonNull ExpirationOptions options,
			@NonNull Collection<@NonNull HK> hashKeys);

	/**
	 * Remove the expiration from given {@code hashKeys} .
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return changes to the hash fields. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	ExpireChanges<@NonNull HK> persist(@NonNull H key, @NonNull Collection<@NonNull HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKeys} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return the actual expirations in seconds for the hash fields. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	default Expirations<@NonNull HK> getTimeToLive(@NonNull H key, Collection<@NonNull HK> hashKeys) {
		return getTimeToLive(key, TimeUnit.SECONDS, hashKeys);
	}

	/**
	 * Get the time to live for {@code hashKeys} and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return the actual expirations for the hash fields. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	Expirations<@NonNull HK> getTimeToLive(@NonNull H key, @NonNull TimeUnit timeUnit,
			@NonNull Collection<@NonNull HK> hashKeys);

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at
	 * {@code key}. Operations on the expiration object obtain keys at the time of invoking any expiration operation.
	 *
	 * @param key must not be {@literal null}.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> expiration(@NonNull H key) {
		return new DefaultBoundHashFieldExpirationOperations<>(this, key, () -> keys(key));
	}

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at
	 * {@code key} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> expiration(@NonNull H key, @NonNull HK @NonNull... hashFields) {
		return expiration(key, Arrays.asList(hashFields));
	}

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at
	 * {@code key} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> expiration(@NonNull H key,
			@NonNull Collection<@NonNull HK> hashFields) {
		return new DefaultBoundHashFieldExpirationOperations<>(this, key, () -> hashFields);
	}



	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<H, ?> getOperations();

}
