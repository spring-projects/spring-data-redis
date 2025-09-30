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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.types.Expiration;

/**
 * Hash operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 * @author Tihomir Mateev
 */
@NullUnmarked
public interface BoundHashOperations<H, HK, HV> extends BoundKeyOperations<H> {

	/**
	 * Delete given hash {@code keys} at the bound key.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long delete(@NonNull Object @NonNull... keys);

	/**
	 * Determine if given hash {@code key} exists at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean hasKey(@NonNull Object key);

	/**
	 * Get value for given {@code key} from the hash at the bound key.
	 *
	 * @param member must not be {@literal null}.
	 * @return {@literal null} when member does not exist or when used in pipeline / transaction.
	 */
	HV get(@NonNull Object member);

	/**
	 * Get values for given {@code keys} from the hash at the bound key. Values are in the order of the requested keys
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> multiGet(@NonNull Collection<@NonNull HK> keys);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long increment(@NonNull HK key, long delta);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Double increment(@NonNull HK key, double delta);

	/**
	 * Return a random key from the hash stored at the bound key.
	 *
	 * @return {@literal null} if the hash does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	HK randomKey();

	/**
	 * Return a random entry from the hash stored at the bound key.
	 *
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map.Entry<@NonNull HK, HV> randomEntry();

	/**
	 * Return a random keys from the hash stored at the bound key. If the provided {@code count} argument is positive,
	 * return a list of distinct keys, capped either at {@code count} or the hash size. If {@code count} is negative, the
	 * behavior changes and the command is allowed to return the same key multiple times. In this case, the number of
	 * returned keys is the absolute value of the specified count.
	 *
	 * @param count number of keys to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	List<@NonNull HK> randomKeys(long count);

	/**
	 * Return a random entry from the hash stored at the bound key.
	 *
	 * @param count number of entries to return. Must be positive.
	 * @return {@literal null} if the hash does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map<@NonNull HK, HV> randomEntries(long count);

	/**
	 * Get key set (fields) of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Set<@NonNull HK> keys();

	/**
	 * Returns the length of the value associated with {@code hashKey}. If the {@code hashKey} do not exist, {@code 0} is
	 * returned.
	 *
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	Long lengthOfValue(@NonNull HK hashKey);

	/**
	 * Get size of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long size();

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m} at the bound key.
	 *
	 * @param m must not be {@literal null}.
	 */
	void putAll(Map<? extends @NonNull HK, ? extends HV> m);

	/**
	 * Set the {@code value} of a hash {@code key} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 */
	void put(@NonNull HK key, HV value);

	/**
	 * Set the {@code value} of a hash {@code key} only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean putIfAbsent(@NonNull HK key, HV value);

	/**
	 * Get entry set (values) of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> values();

	/**
	 * Get entire hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Map<@NonNull HK, HV> entries();

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at the bound key. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	Cursor<Map.Entry<@NonNull HK, HV>> scan(@NonNull ScanOptions options);

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@code key}. Operations on the expiration object obtain keys at the time of invoking any expiration
	 * operation.
	 *
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> hashExpiration() {
		return new DefaultBoundHashFieldExpirationOperations<>(getOperations().opsForHash(), getKey(), this::keys);
	}

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@code key} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> hashExpiration(@NonNull HK @NonNull... hashFields) {
		return hashExpiration(Arrays.asList(hashFields));
	}

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@code key} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	@NonNull
	default BoundHashFieldExpirationOperations<HK> hashExpiration(@NonNull Collection<@NonNull HK> hashFields) {
		return new DefaultBoundHashFieldExpirationOperations<>(getOperations().opsForHash(), getKey(), () -> hashFields);
	}

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<H, ?> getOperations();

	/**
	 * Get and remove the value for given {@code hashFields} from the hash at the bound key. Values are in the order of the
	 * requested hash fields. Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param hashFields must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.1
	 */
    List<HV> getAndDelete(@NonNull Collection<@NonNull HK> hashFields);

    /**
     * Get and optionally expire the value for given {@code hashFields} from the hash at the bound key. Values are in the order of the
     * requested hash fields. Absent field values are represented using {@literal null} in the resulting {@link List}.
     *
     * @param expiration is optional.
     * @param hashFields must not be {@literal null}.
     * @return never {@literal null}.
     * @since 4.0
     */
    List<HV> getAndExpire(Expiration expiration, @NonNull Collection<@NonNull HK> hashFields);

    /**
     * Set the value of one or more fields using data provided in {@code m} at the bound key, and optionally set their
     * expiration time or time-to-live (TTL). The {@code condition} determines whether the fields are set.
     *
     * @param m must not be {@literal null}.
     * @param condition is optional. Use {@link RedisHashCommands.HashFieldSetOption#IF_NONE_EXIST} (FNX) to only set the fields if
     *                  none of them already exist, {@link RedisHashCommands.HashFieldSetOption#IF_ALL_EXIST} (FXX) to only set the
     *                  fields if all of them already exist, or {@link RedisHashCommands.HashFieldSetOption#UPSERT} to set the fields
     *                  unconditionally.
     * @param expiration is optional.
     */
    void putAndExpire(Map<? extends @NonNull HK, ? extends HV> m, RedisHashCommands.HashFieldSetOption condition, Expiration expiration);
}
