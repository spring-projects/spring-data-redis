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
 * Redis map specific operations working on a hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Tihomir Mateev
 */
public interface HashOperations<H, HK, HV> {

	/**
	 * Delete given hash {@code hashKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long delete(H key, Object... hashKeys);

	/**
	 * Determine if given hash {@code hashKey} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean hasKey(H key, Object hashKey);

	/**
	 * Get value for given {@code hashKey} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when key or hashKey does not exist or used in pipeline / transaction.
	 */
	@Nullable
	HV get(H key, Object hashKey);

	/**
	 * Get values for given {@code hashKeys} from hash at {@code key}. Values are in the order of the requested keys
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> multiGet(H key, Collection<HK> hashKeys);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long increment(H key, HK hashKey, long delta);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Double increment(H key, HK hashKey, double delta);

	/**
	 * Return a random hash key (aka field) from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	HK randomKey(H key);

	/**
	 * Return a random entry from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map.Entry<HK, HV> randomEntry(H key);

	/**
	 * Return random hash keys (aka fields) from the hash stored at {@code key}. If the provided {@code count} argument is
	 * positive, return a list of distinct hash keys, capped either at {@code count} or the hash size. If {@code count} is
	 * negative, the behavior changes and the command is allowed to return the same hash key multiple times. In this case,
	 * the number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	List<HK> randomKeys(H key, long count);

	/**
	 * Return a random entries from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return. Must be positive.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map<HK, HV> randomEntries(H key, long count);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Set<HK> keys(H key);

	/**
	 * Returns the length of the value associated with {@code hashKey}. If either the {@code key} or the {@code hashKey}
	 * do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	@Nullable
	Long lengthOfValue(H key, HK hashKey);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long size(H key);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m}.
	 *
	 * @param key must not be {@literal null}.
	 * @param m must not be {@literal null}.
	 */
	void putAll(H key, Map<? extends HK, ? extends HV> m);

	/**
	 * Set the {@code value} of a hash {@code hashKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 */
	void put(H key, HK hashKey, HV value);

	/**
	 * Set the {@code value} of a hash {@code hashKey} only if {@code hashKey} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean putIfAbsent(H key, HK hashKey, HV value);

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> values(H key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Map<HK, HV> entries(H key);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	Cursor<Map.Entry<HK, HV>> scan(H key, ScanOptions options);

	/**
	 * Set time to live for given {@code hashKey} (aka field).
	 *
	 * @param key must not be {@literal null}.
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
	List<Long> expire(H key, Duration timeout, Collection<HK> hashKeys);

	/**
	 * Set the expiration for given {@code hashKey} (aka field) as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
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
	List<Long> expireAt(H key, Instant expireAt, Collection<HK> hashKeys);

	/**
	 * Remove the expiration from given {@code hashKey} (aka field).
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is removed;
	 * {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such field; {@literal null} when
	 * used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> persist(H key, Collection<HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKey} (aka field) in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative value
	 * to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 * returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> getExpire(H key, Collection<HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKey} (aka field) and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative value
	 * to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time. The command
	 * returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	List<Long> getExpire(H key, TimeUnit timeUnit, Collection<HK> hashKeys);
	/**
	 * @return never {@literal null}.
	 */
	RedisOperations<H, ?> getOperations();
}
