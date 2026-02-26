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

/**
 * Builder for delete operations.
 *
 * @param <K> key type.
 * @param <V> value type.
 * @author Mark Paluch
 * @since 4.1
 */
public interface DeleteOperationBuilder<K, V> {

	/**
	 * Delete the key without any condition.
	 *
	 * @return this builder.
	 */
	DeleteOperationBuilder<K, V> always();

	/**
	 * Configure deletion only if the value matches the given value or digest.
	 *
	 * @return a {@link ComparisonSpec} to specify the value or digest to compare against.
	 */
	ComparisonSpec<K, V> ifEquals();

	/**
	 * Configure deletion only if the value does not match the given value or digest.
	 *
	 * @return a {@link ComparisonSpec} to specify the value or digest to compare against.
	 */
	ComparisonSpec<K, V> ifNotEquals();

	/**
	 * Steps to customize value or digest comparison for conditional delete operations.
	 *
	 * @param <K> key type.
	 * @param <V> value type.
	 */
	interface ComparisonSpec<K, V> {

		/**
		 * Configure value comparison for conditional delete operations.
		 *
		 * @param value the value to compare against.
		 * @return the previously used {@code DeleteOperationBuilder}.
		 */
		DeleteOperationBuilder<K, V> value(V value);

		/**
		 * Configure digest comparison for conditional delete operations.
		 *
		 * @param hex16 hex representation of the digest to compare against. Can be obtained through Redis' {@code DIGEST}
		 *          command.
		 * @return the previously used {@code DeleteOperationBuilder}.
		 */
		DeleteOperationBuilder<K, V> digest(String hex16);

	}

}
