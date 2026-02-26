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

import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.CompareCondition;
import org.springframework.util.Assert;

/**
 * Builder for delete operations.
 *
 * @author Mark Paluch
 * @since 4.1
 */
class DefaultDeleteOperationsBuilder<K, V>
		implements DeleteOperationBuilder<K, V>, DeleteOperationBuilder.ComparisonSpec<K, V> {

	private @Nullable V value;

	private CompareCondition.@Nullable Comparison comparison;

	private boolean equal;

	private @Nullable String digest;

	@Override
	public DeleteOperationBuilder<K, V> always() {
		this.comparison = null;
		return this;
	}

	@Override
	public ComparisonSpec<K, V> ifEquals() {
		this.equal = true;
		return this;
	}

	@Override
	public ComparisonSpec<K, V> ifNotEquals() {
		this.equal = false;
		return this;
	}

	@Override
	public DeleteOperationBuilder<K, V> value(V value) {
		this.comparison = CompareCondition.Comparison.VALUE;
		this.value = value;
		return this;
	}

	@Override
	public DeleteOperationBuilder<K, V> digest(String hex16) {
		this.comparison = CompareCondition.Comparison.DIGEST;
		this.value = null;
		this.digest = hex16;
		return this;
	}

	/**
	 * Create a {@link CompareCondition} based on the configured comparison type and value.
	 *
	 * @param serializer serializer function to serialize the value for by-value comparison, must not be {@code null}.
	 * @return a {@link CompareCondition} or {@literal null} if no comparison (i.e. ALWAYS) is configured.
	 */
	public @Nullable CompareCondition toCompareCondition(Function<V, byte[]> serializer) {

		if (comparison == null) {
			return null;
		}

		return switch (comparison) {
			case VALUE -> {
				Assert.notNull(value, "Value must not be null");
				yield equal ? CompareCondition.ifEquals(serializer.apply(value))
						: CompareCondition.ifNotEquals(serializer.apply(value));
			}
			case DIGEST -> {
				Assert.notNull(digest, "Digest must not be null");
				yield equal ? CompareCondition.ifDigestEquals(this.digest) : CompareCondition.ifDigestNotEquals(this.digest);
			}
		};
	}

}
