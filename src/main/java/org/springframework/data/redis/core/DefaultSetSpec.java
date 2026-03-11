/*
 * Copyright 2026-present the original author or authors.
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
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.CompareCondition;
import org.springframework.data.redis.connection.SetCondition;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * Builder for set operations.
 *
 * @author Yordan Tsintsov
 * @since 4.1
 */
class DefaultSetSpec<K, V> implements SetSpec<K, V>, SetSpec.ComparisonSpec<K, V> {

	private @Nullable V value;
	private SetCondition.@Nullable KeyCondition keyCondition;
	private CompareCondition.@Nullable ComparisonFunction comparison;
	private boolean equal;
	private Expiration expiration = Expiration.persistent();
	private @Nullable String digest = null;

	@Override
	public SetSpec<K, V> always() {
		this.keyCondition = SetCondition.KeyCondition.UPSERT;
		this.comparison = null;
		return this;
	}

	@Override
	public SetSpec<K, V> ifAbsent() {
		this.keyCondition = SetCondition.KeyCondition.IF_ABSENT;
		this.comparison = null;
		return this;
	}

	@Override
	public SetSpec<K, V> ifPresent() {
		this.keyCondition = SetCondition.KeyCondition.IF_PRESENT;
		this.comparison = null;
		return this;
	}

	@Override
	public ComparisonSpec<K, V> ifEquals() {
		this.keyCondition = null;
		this.equal = true;
		return this;
	}

	@Override
	public ComparisonSpec<K, V> ifNotEquals() {
		this.keyCondition = null;
		this.equal = false;
		return this;
	}

	@Override
	public SetSpec<K, V> keepTtl() {
		this.expiration = Expiration.keepTtl();
		return this;
	}

	@Override
	public SetSpec<K, V> expiration(Expiration expiration) {
		this.expiration = expiration;
		return this;
	}

	@Override
	public SetSpec<K, V> expire(Duration timeout) {
		this.expiration = Expiration.from(timeout);
		return this;
	}

	@Override
	public SetSpec<K, V> value(V value) {
		this.keyCondition = null;
		this.comparison = CompareCondition.ComparisonFunction.VALUE;
		this.value = value;
		this.digest = null;
		return this;
	}

	@Override
	public SetSpec<K, V> digest(String hex16) {
		this.keyCondition = null;
		this.comparison = CompareCondition.ComparisonFunction.DIGEST;
		this.value = null;
		this.digest = hex16;
		return this;
	}

	public SetCondition toSetCondition(Function<V, byte[]> serializer) {

		if (comparison != null) {
			return switch (comparison) {
				case VALUE -> {
					Assert.notNull(value, "Value must not be null");
					yield equal ? SetCondition.ifEquals(serializer.apply(value))
							: SetCondition.ifNotEquals(serializer.apply(value));
				}
				case DIGEST -> {
					Assert.notNull(digest, "Digest must not be null");
					yield equal ? SetCondition.ifDigestEquals(this.digest) : SetCondition.ifDigestNotEquals(this.digest);
				}
			};
		}

		if (keyCondition != null) {
			return switch (keyCondition) {
				case UPSERT -> SetCondition.upsert();
				case IF_ABSENT -> SetCondition.ifAbsent();
				case IF_PRESENT -> SetCondition.ifPresent();
			};
		}

		return SetCondition.upsert();
	}

	public Expiration getExpiration() {
		return this.expiration;
	}

}
