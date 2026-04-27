/*
 * Copyright 2026 the original author or authors.
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
 * Strategy for resolving a String destination name to an actual Redis {@link Topic topic}.
 *
 * @author Mark Paluch
 * @since 4.1
 * @see ChannelTopic
 * @see PatternTopic
 */
@FunctionalInterface
public interface TopicResolver<T extends Topic> {

	/**
	 * Resolve the given topic name.
	 *
	 * @param name the topic name to resolve.
	 * @return the resolved topic.
	 */
	T resolveTopic(String name);

	/**
	 * Return a {@link TopicResolver} for resolving {@link ChannelTopic} for a given topic name.
	 */
	static TopicResolver<ChannelTopic> channel() {
		return TopicResolvers.channel();
	}

	/**
	 * Return a {@link TopicResolver} for resolving {@link PatternTopic} for a given topic name.
	 */
	static TopicResolver<PatternTopic> pattern() {
		return TopicResolvers.pattern();
	}

}
