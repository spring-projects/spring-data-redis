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

import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.Topic;
import org.springframework.util.ConcurrentLruCache;

/**
 * A simple {@link TopicResolver} implementation for channel and pattern resolution.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class SimpleTopicResolver implements TopicResolver {

	private final ConcurrentLruCache<String, ChannelTopic> channelCache = new ConcurrentLruCache<>(32, ChannelTopic::of);

	private final ConcurrentLruCache<String, PatternTopic> patternCache = new ConcurrentLruCache<>(32, PatternTopic::of);

	@Override
	public Topic resolveTopic(String destinationName) {
		return containsPatternGlobs(destinationName) ? patternCache.get(destinationName)
				: channelCache.get(destinationName);
	}

	private static boolean containsPatternGlobs(String destinationName) {
		return containsGlob(destinationName, "?") || containsGlob(destinationName, "*")
				|| containsGlob(destinationName, "[");
	}

	private static boolean containsGlob(String destinationName, String glob) {
		return destinationName.contains(glob) && !destinationName.contains("\\" + glob);
	}
}
