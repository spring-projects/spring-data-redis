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

/**
 * A simple {@link TopicResolver} implementation for channel and pattern resolution.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.1
 */
public class SimpleTopicResolver implements TopicResolver<Topic> {

	private final CachingTopicResolver<ChannelTopic> channels = new CachingTopicResolver<>(32, TopicResolver.channel());

	private final CachingTopicResolver<PatternTopic> patterns = new CachingTopicResolver<>(32, TopicResolver.pattern());

	@Override
	public Topic resolveTopic(String name) {
		return containsPatternGlobs(name) ? patterns.resolveTopic(name) : channels.resolveTopic(name);
	}

	/**
	 * @return {@literal true} if {@code destinationName} contains an unescaped glob meta character.
	 */
	private static boolean containsPatternGlobs(String destinationName) {

		for (int i = 0; i < destinationName.length(); i++) {

			char c = destinationName.charAt(i);
			if (c == '\\') {
				i++;
				continue;
			}
			if (c == '?' || c == '*' || c == '[') {
				return true;
			}
		}
		return false;
	}
}
