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
package org.springframework.data.redis.connection;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * {@literal HyperLogLog} specific commands supported by Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.5
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisHyperLogLogCommands {

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 */
	Long pfAdd(byte @NonNull [] key, byte @NonNull [] @NonNull... values);

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 */
	Long pfCount(byte @NonNull [] @NonNull... keys);

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sourceKeys must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/pfmerge">Redis Documentation: PFMERGE</a>
	 */
	void pfMerge(byte @NonNull [] destinationKey, byte @NonNull [] @NonNull... sourceKeys);

}
