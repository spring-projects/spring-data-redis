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

import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.lang.Nullable;

/**
 * Key Expiration operations bound to a key.
 *
 * @author Mark Paluch
 * @since 3.5
 */
public interface BoundKeyExpirationOperations {

	/**
	 * Apply {@link Expiration} to the bound key without any additional constraints.
	 *
	 * @param expiration the expiration definition.
	 * @return changes to the key. {@literal null} when used in pipeline / transaction.
	 */
	default ExpireChanges.ExpiryChangeState expire(Expiration expiration) {
		return expire(expiration, ExpirationOptions.none());
	}

	/**
	 * Apply {@link Expiration} to the bound key given {@link ExpirationOptions expiration options}.
	 *
	 * @param expiration the expiration definition.
	 * @param options expiration options.
	 * @return changes to the key. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	ExpireChanges.ExpiryChangeState expire(Expiration expiration, ExpirationOptions options);

	/**
	 * Set time to live for the bound key.
	 *
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
	 * @return changes to the key. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/expire/">Redis Documentation: EXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges.ExpiryChangeState expire(Duration timeout);

	/**
	 * Set the expiration for the bound key as a {@literal date} timestamp.
	 *
	 * @param expireAt must not be {@literal null}.
	 * @return changes to the key. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/expireat/">Redis Documentation: EXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges.ExpiryChangeState expireAt(Instant expireAt);

	/**
	 * Remove the expiration from the bound key.
	 *
	 * @return changes to the key. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/persist/">Redis Documentation: PERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	ExpireChanges.ExpiryChangeState persist();

	/**
	 * Get the time to live for the bound key in seconds.
	 *
	 * @return the actual expirations in seconds for the key. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/ttl/">Redis Documentation: TTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations.TimeToLive getTimeToLive();

	/**
	 * Get the time to live for the bound key and convert it to the given {@link TimeUnit}.
	 *
	 * @param timeUnit must not be {@literal null}.
	 * @return the actual expirations for the key in the given time unit. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/ttl/">Redis Documentation: TTL</a>
	 * @since 3.5
	 */
	@Nullable
	Expirations.TimeToLive getTimeToLive(TimeUnit timeUnit);

}
