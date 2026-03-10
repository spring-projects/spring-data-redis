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

import org.springframework.data.redis.core.types.Expiration;

/**
 * Steps for configuring set operations.
 *
 * @param <K> key type.
 * @param <V> value type.
 * @author Yordan Tsintsov
 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
 * @since 4.1
 */
public interface SetSpec<K, V> {

	/**
	 * Set the key unconditionally.
	 *
	 * @return this builder.
	 */
	SetSpec<K, V> always();

	/**
	 * Set the key if it does not exist.
	 *
	 * @return this builder.
	 */
	SetSpec<K, V> ifAbsent();

	/**
	 * Set the key if it exists.
	 *
	 * @return this builder.
	 */
	SetSpec<K, V> ifPresent();

	/**
	 * Configure set only if the value matches the given value or digest.
	 *
	 * @return a {@link ComparisonSpec} to specify the value or digest to compare against.
	 */
	ComparisonSpec<K, V> ifEquals();

	/**
	 * Configure set only if the value does not match the given value or digest.
	 *
	 * @return a {@link ComparisonSpec} to specify the value or digest to compare against.
	 */
	ComparisonSpec<K, V> ifNotEquals();

	/**
	 * Retain the existing TTL associated with the key.
	 *
	 * @return this builder.
	 */
	SetSpec<K, V> keepTtl();

	/**
	 * Set the key with the given expiration.
	 *
	 * @param expiration the expiration to apply.
	 * @return this builder.
	 */
	SetSpec<K, V> expiration(Expiration expiration);

	/**
	 * Set the key to expire after the given duration.
	 *
	 * @param timeout the duration after which the key expires.
	 * @return this builder.
	 */
	SetSpec<K, V> timeout(Duration timeout);

	/**
	 * Steps to customize value or digest comparison for conditional set operations.
	 *
	 * @param <K> key type.
	 * @param <V> value type.
	 */
	interface ComparisonSpec<K, V> {

		/**
		 * Configure value comparison for conditional set operations.
		 *
		 * @param value the value to compare against.
		 * @return the previously used {@code SetSpec}.
		 */
		SetSpec<K, V> value(V value);

		/**
		 * Configure digest comparison for conditional set operations.
		 *
		 * @param hex16 hex representation of the digest to compare against. Can be obtained through Redis' {@code DIGEST}
		 *              command.
		 * @return the previously used {@code SetSpec}.
		 */
		SetSpec<K, V> digest(String hex16);

	}

}
