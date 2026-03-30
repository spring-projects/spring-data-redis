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
package org.springframework.data.redis.listener.support;

import org.springframework.data.redis.listener.Topic;
import org.springframework.util.ConcurrentLruCache;

/**
 * Caching {@link TopicResolver} variant using a {@link TopicResolver} for the actual resolution.
 * <p>
 * This resolve uses a {@link ConcurrentLruCache} to cache resolved topics.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class CachingTopicResolver<T extends Topic> implements TopicResolver<T> {

	private final ConcurrentLruCache<String, T> cache;

	/**
	 * Create a new {@code CachingTopicResolver} with the given {@link TopicResolver} and cache size.
	 *
	 * @param cacheSize cache size.
	 * @param resolver the actual resolver to resolve topics if not found in cache; must not be {@literal null}.
	 */
	public CachingTopicResolver(int cacheSize, TopicResolver<T> resolver) {
		this.cache = new ConcurrentLruCache<>(cacheSize, resolver::resolveTopic);
	}

	@Override
	public T resolveTopic(String name) {
		return cache.get(name);
	}

}
