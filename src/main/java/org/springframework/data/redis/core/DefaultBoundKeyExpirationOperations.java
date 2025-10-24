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

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;

/**
 * Default {@link BoundKeyExpirationOperations} implementation.
 *
 * @author Mark Paluch
 * @since 3.5
 */
class DefaultBoundKeyExpirationOperations<K> implements BoundKeyExpirationOperations {

	private final RedisOperations<K, ?> operations;
	private final K key;

	public DefaultBoundKeyExpirationOperations(RedisOperations<K, ?> operations, K key) {
		this.operations = operations;
		this.key = key;
	}

	@Override
	public ExpireChanges.@Nullable ExpiryChangeState expire(Expiration expiration, ExpirationOptions options) {
		return operations.expire(key, expiration, options);
	}

	@Override
	public ExpireChanges.@Nullable ExpiryChangeState expire(Duration timeout) {

		Boolean expire = operations.expire(key, timeout);

		return toExpiryChangeState(expire);
	}

	@Override
	public ExpireChanges.@Nullable ExpiryChangeState expireAt(Instant expireAt) {
		return toExpiryChangeState(operations.expireAt(key, expireAt));
	}

	@Override
	public ExpireChanges.@Nullable ExpiryChangeState persist() {
		return toExpiryChangeState(operations.persist(key));
	}

	@Override
	public Expirations.@Nullable TimeToLive getTimeToLive() {

		Long expire = operations.getExpire(key);

		return expire == null ? null : Expirations.TimeToLive.of(expire, TimeUnit.SECONDS);
	}

	@Override
	public Expirations.@Nullable TimeToLive getTimeToLive(TimeUnit timeUnit) {

		Long expire = operations.getExpire(key, timeUnit);

		return expire == null ? null : Expirations.TimeToLive.of(expire, timeUnit);

	}

	private static ExpireChanges.@Nullable ExpiryChangeState toExpiryChangeState(@Nullable Boolean result) {

		if (result == null) {
			return null;
		}

		return ExpireChanges.ExpiryChangeState.of(result);
	}

}
