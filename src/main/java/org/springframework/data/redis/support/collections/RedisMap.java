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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.BoundHashFieldExpirationOperations;

/**
 * Map view of a Redis hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Tihomi Mateev
 * @author Mark Paluch
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
	Map.@Nullable Entry<K, V> randomEntry();

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<Map.Entry<K, V>> scan();

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@link #getKey()}. Operations on the expiration object obtain keys at the time of invoking any expiration
	 * operation.
	 *
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	BoundHashFieldExpirationOperations<K> hashFieldExpiration();

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@link #getKey()} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	default BoundHashFieldExpirationOperations<K> hashFieldExpiration(K... hashFields) {
		return hashFieldExpiration(Arrays.asList(hashFields));
	}

	/**
	 * Returns a bound operations object to perform operations on the hash field expiration for all hash fields at the
	 * bound {@link #getKey()} for the given hash fields.
	 *
	 * @param hashFields collection of hash fields to operate on.
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	BoundHashFieldExpirationOperations<K> hashFieldExpiration(Collection<K> hashFields);

}
