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
package org.springframework.data.redis.support.collections;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.ExpireChanges;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.lang.Nullable;

/**
 * Map view of a Redis hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Tihomi Mateev
 */
public interface RedisMap<K, V> extends RedisStore, ConcurrentMap<K, V> {

	/**
	 * Increment {@code value} of the hash {@code key} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} if hash does not exist.
	 * @since 1.0
	 */
	Long increment(K key, long delta);

	/**
	 * Increment {@code value} of the hash {@code key} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} if hash does not exist.
	 * @since 1.1
	 */
	Double increment(K key, double delta);

	/**
	 * Get a random key from the hash.
	 *
	 * @return {@literal null} if the hash does not exist.
	 * @since 2.6
	 */
	K randomKey();

	/**
	 * Get a random entry from the hash.
	 *
	 * @return {@literal null} if the hash does not exist.
	 * @since 2.6
	 */
	@Nullable
	Map.Entry<K, V> randomEntry();

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<Map.Entry<K, V>> scan();

	/**
	 * Set time to live for given {@code hashKeys}.
	 *
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline /
	 *         transaction.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges<K> expire(Duration timeout, Collection<K> hashKeys);

	/**
	 * Set the expiration for given {@code hashKeys} as a {@literal date} timestamp.
	 *
	 * @param expireAt must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field; {@literal null} when used in
	 *         pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges<K> expireAt(Instant expireAt, Collection<K> hashKeys);

	/**
	 * Remove the expiration from given {@code hashKeys}.
	 *
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is
	 *         removed; {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges<K> persist(Collection<K> hashKeys);

	/**
	 * Get the time to live for {@code hashKeys} in seconds.
	 *
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations<K> getTimeToLive(Collection<K> hashKeys);

	/**
	 * Get the time to live for {@code hashKeys} and convert it to the given {@link TimeUnit}.
	 *
	 * @param timeUnit must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations<K> getTimeToLive(TimeUnit timeUnit, Collection<K> hashKeys);

}
