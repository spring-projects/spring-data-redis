/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.cache;

import org.springframework.data.redis.connection.RedisConnection;

/**
 * A {@link BatchStrategy} to be used with {@link RedisCacheWriter}.
 * <p>
 * Mainly used to clear the cache.
 * <p>
 * Predefined strategies using the {@link BatchStrategies#keys() KEYS} or {@link BatchStrategies#scan(int) SCAN}
 * commands can be found in {@link BatchStrategies}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.6
 */
public interface BatchStrategy {

	/**
	 * Remove all keys following the given pattern.
	 *
	 * @param connection the connection to use. Must not be {@literal null}.
	 * @param name The cache name. Must not be {@literal null}.
	 * @param pattern The pattern for the keys to remove. Must not be {@literal null}.
	 * @return number of removed keys.
	 */
	long cleanCache(RedisConnection connection, String name, byte[] pattern);

}
