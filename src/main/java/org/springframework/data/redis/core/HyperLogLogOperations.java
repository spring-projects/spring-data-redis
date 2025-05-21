/*
 * Copyright 2014-2025 the original author or authors.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * @author Christoph Strobl
 * @since 1.5
 */
@NullUnmarked
public interface HyperLogLogOperations<K, V> {

	/**
	 * Adds the given {@literal values} to the {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return 1 of at least one of the values was added to the key; 0 otherwise. {@literal null} when used in pipeline /
	 *         transaction.
	 */
	Long add(@NonNull K key, @NonNull V @NonNull... values);

	/**
	 * Gets the current number of elements within the {@literal key}.
	 *
	 * @param keys must not be {@literal null} or {@literal empty}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long size(@NonNull K @NonNull... keys);

	/**
	 * Merges all values of given {@literal sourceKeys} into {@literal destination} key.
	 *
	 * @param destination key of HyperLogLog to move source keys into.
	 * @param sourceKeys must not be {@literal null} or {@literal empty}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long union(@NonNull K destination, @NonNull K @NonNull... sourceKeys);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	void delete(@NonNull K key);

}
