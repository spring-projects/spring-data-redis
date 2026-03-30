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

/**
 * {@link TopicResolver} implementations.
 *
 * @author Mark Paluch
 * @since 4.1
 */
class TopicResolvers {

	/**
	 * Return a {@link TopicResolver} that resolves {@link ChannelTopic}s.
	 */
	public static TopicResolver<ChannelTopic> channel() {
		return ChannelTopicResolver.CHANNEL;
	}

	/**
	 * Return a {@link TopicResolver} that resolves {@link PatternTopic}s.
	 */
	public static TopicResolver<PatternTopic> pattern() {
		return PatternTopicResolver.PATTERN;
	}

	private enum ChannelTopicResolver implements TopicResolver<ChannelTopic> {

		CHANNEL {
			@Override
			public ChannelTopic resolveTopic(String name) {
				return ChannelTopic.of(name);
			}
		}

	}

	private enum PatternTopicResolver implements TopicResolver<PatternTopic> {

		PATTERN {
			@Override
			public PatternTopic resolveTopic(String name) {
				return PatternTopic.of(name);
			}
		}

	}

}
