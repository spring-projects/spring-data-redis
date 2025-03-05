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
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link BoundHashFieldExpirationOperations}.
 *
 * @author Mark Paluch
 * @since 3.5
 */
class DefaultBoundHashFieldExpirationOperations<H, HK> implements BoundHashFieldExpirationOperations<HK> {

	private final HashOperations<H, HK, ?> operations;
	private final H key;
	private final Supplier<? extends Collection<HK>> hashFields;

	public DefaultBoundHashFieldExpirationOperations(HashOperations<H, HK, ?> operations, H key,
			Supplier<? extends Collection<HK>> hashFields) {

		this.operations = operations;
		this.key = key;
		this.hashFields = hashFields;
	}

	@Override
	public ExpireChanges<HK> expire(Expiration expiration, ExpirationOptions options) {
		return operations.expire(key, expiration, options, getHashKeys());
	}

	@Nullable
	@Override
	public ExpireChanges<HK> expire(Duration timeout) {
		return operations.expire(key, timeout, getHashKeys());
	}

	@Nullable
	@Override
	public ExpireChanges<HK> expireAt(Instant expireAt) {
		return operations.expireAt(key, expireAt, getHashKeys());
	}

	@Nullable
	@Override
	public ExpireChanges<HK> persist() {
		return operations.persist(key, getHashKeys());
	}

	@Nullable
	@Override
	public Expirations<HK> getTimeToLive() {
		return operations.getTimeToLive(key, getHashKeys());
	}

	@Nullable
	@Override
	public Expirations<HK> getTimeToLive(TimeUnit timeUnit) {
		return operations.getTimeToLive(key, timeUnit, getHashKeys());
	}

	private Collection<HK> getHashKeys() {

		Collection<HK> hks = hashFields.get();

		Assert.state(hks != null, "Hash keys must not be null");
		return hks;
	}

}
