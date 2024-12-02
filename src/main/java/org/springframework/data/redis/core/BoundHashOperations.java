/*
 * Copyright 2011-2024 the original author or authors.
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;

/**
 * Hash operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 * @author Tihomir Mateev
 */
public interface BoundHashOperations<H, HK, HV> extends BoundKeyOperations<H> {

	/**
	 * Delete given hash {@code keys} at the bound key.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Long delete(Object... keys);

	/**
	 * Determine if given hash {@code key} exists at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean hasKey(Object key);

	/**
	 * Get value for given {@code key} from the hash at the bound key.
	 *
	 * @param member must not be {@literal null}.
	 * @return {@literal null} when member does not exist or when used in pipeline / transaction.
	 */
	@Nullable
	HV get(Object member);

	/**
	 * Get values for given {@code keys} from the hash at the bound key. Values are in the order of the requested keys
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	List<HV> multiGet(Collection<HK> keys);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Long increment(HK key, long delta);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Double increment(HK key, double delta);

	/**
	 * Return a random key (aka field) from the hash stored at the bound key.
	 *
	 * @return {@literal null} if the hash does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	HK randomKey();

	/**
	 * Return a random entry from the hash stored at the bound key.
	 *
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map.Entry<HK, HV> randomEntry();

	/**
	 * Return a random keys (aka fields) from the hash stored at the bound key. If the provided {@code count} argument is
	 * positive, return a list of distinct keys, capped either at {@code count} or the hash size. If {@code count} is
	 * negative, the behavior changes and the command is allowed to return the same key multiple times. In this case, the
	 * number of returned keys is the absolute value of the specified count.
	 *
	 * @param count number of keys to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	List<HK> randomKeys(long count);

	/**
	 * Return a random entry from the hash stored at the bound key.
	 *
	 * @param count number of entries to return. Must be positive.
	 * @return {@literal null} if the hash does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map<HK, HV> randomEntries(long count);

	/**
	 * Get key set (fields) of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Set<HK> keys();

	/**
	 * Returns the length of the value associated with {@code hashKey}. If the {@code hashKey} do not exist, {@code 0} is
	 * returned.
	 *
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	@Nullable
	Long lengthOfValue(HK hashKey);

	/**
	 * Set time to live for given {@code hashKey} (aka field).
	 *
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 * already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time is set/updated;
	 * {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met); {@code -2}
	 * indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> expire(Duration timeout, Collection<HK> hashKeys);

	/**
	 * Set the expiration for given {@code hashKey} (aka field) as a {@literal date} timestamp.
	 *
	 * @param expireAt must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 * already due to expiration, or provided expiry interval is in the past; {@code 1} indicating expiration time is
	 * set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met);
	 * {@code -2} indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> expireAt(Instant expireAt, Collection<HK> hashKeys);

	/**
	 * Remove the expiration from given {@code hashKey} (aka field).
	 *
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is removed;
	 * {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such field; {@literal null} when
	 * used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> persist(Collection<HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKey} (aka field) in seconds.
	 *
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative value
	 * to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 * returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> getExpire(Collection<HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKey} (aka field) and convert it to the given {@link TimeUnit}.
	 *
	 * @param timeUnit must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative value
	 * to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 * returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> getExpire(TimeUnit timeUnit, Collection<HK> hashKeys);

	/**
	 * Get size of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Long size();

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m} at the bound key.
	 *
	 * @param m must not be {@literal null}.
	 */
	void putAll(Map<? extends HK, ? extends HV> m);

	/**
	 * Set the {@code value} of a hash {@code key} at the bound key.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 */
	void put(HK key, HV value);

	/**
	 * Set the {@code value} of a hash {@code key} only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean putIfAbsent(HK key, HV value);

	/**
	 * Get entry set (values) of hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	List<HV> values();

	/**
	 * Get entire hash at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Map<HK, HV> entries();

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at the bound key. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	Cursor<Map.Entry<HK, HV>> scan(ScanOptions options);

	/**
	 * @return never {@literal null}.
	 */
	RedisOperations<H, ?> getOperations();
}
