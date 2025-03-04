/*
 * Copyright 2025 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.Hash;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.lang.Nullable;

/**
 * Hash Field Expiration operations bound to a certain hash key and set of hash fields.
 *
 * @param <HK> type of the hash field names.
 * @author Mark Paluch
 * @since 3.5
 */
public interface BoundHashFieldExpirationOperations<HK> {

	/**
	 * Apply {@link Expiration} to the hash without any additional constraints.
	 *
	 * @param expiration the expiration definition.
	 * @return changes to the hash fields.
	 */
	default ExpireChanges<HK> expire(Expiration expiration) {
		return expire(expiration, Hash.FieldExpirationOptions.none());
	}

	/**
	 * Apply {@link Expiration} to the hash fields given {@link Hash.FieldExpirationOptions expiration options}.
	 *
	 * @param expiration the expiration definition.
	 * @param options expiration options.
	 * @return changes to the hash fields.
	 */
	ExpireChanges<HK> expire(Expiration expiration, Hash.FieldExpirationOptions options);

	/**
	 * Set time to live for given {@code hashKey}.
	 *
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
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
	ExpireChanges<HK> expire(Duration timeout);

	/**
	 * Set the expiration for given {@code hashKey} as a {@literal date} timestamp.
	 *
	 * @param expireAt must not be {@literal null}.
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
	ExpireChanges<HK> expireAt(Instant expireAt);

	/**
	 * Remove the expiration from given {@code hashKey} .
	 *
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is
	 *         removed; {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges<HK> persist();

	/**
	 * Get the time to live for {@code hashKey} in seconds.
	 *
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations<HK> getTimeToLive();

	/**
	 * Get the time to live for {@code hashKey} and convert it to the given {@link TimeUnit}.
	 *
	 * @param timeUnit must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations<HK> getTimeToLive(TimeUnit timeUnit);

}
