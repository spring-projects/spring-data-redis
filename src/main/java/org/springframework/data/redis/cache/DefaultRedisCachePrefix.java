/*
 * Copyright 2011 the original author or authors.
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

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Default implementation for {@link RedisCachePrefix} which uses the given cache name and a delimiter for creating the prefix.
 * 
 * @author Costin Leau
 */
public class DefaultRedisCachePrefix implements RedisCachePrefix {

	private final RedisSerializer serializer = new StringRedisSerializer();
	private final String delimiter;

	public DefaultRedisCachePrefix() {
		this(":");
	}

	public DefaultRedisCachePrefix(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public byte[] prefix(String cacheName) {
		return serializer.serialize((delimiter != null ? cacheName.concat(":") : cacheName));
	}
}
