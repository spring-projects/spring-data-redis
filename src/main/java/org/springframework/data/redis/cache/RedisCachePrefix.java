/*
 * Copyright 2011-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.redis.cache;

/**
 * Contract for generating 'prefixes' for Cache keys saved in Redis. Due to the 'flat' nature of the Redis storage, the
 * prefix is used as a 'namespace' for grouping the key/values inside a cache (and to avoid collision with other caches
 * or keys inside Redis).
 * 
 * @author Costin Leau
 */
public interface RedisCachePrefix {

	/**
	 * Returns the prefix for the given cache (identified by name). Note the prefix is returned in raw form so it can be
	 * saved directly to Redis without any serialization.
	 * 
	 * @param cacheName the name of the cache using the prefix
	 * @return the prefix for the given cache.
	 */
	byte[] prefix(String cacheName);

}
