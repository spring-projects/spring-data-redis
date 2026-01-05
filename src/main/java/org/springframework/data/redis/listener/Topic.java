/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.listener;

/**
 * Topic for a Redis message. Acts a high-level abstraction on top of Redis low-level channels or patterns.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public interface Topic {

	/**
	 * Create a new {@link ChannelTopic} for channel subscriptions.
	 *
	 * @param channelName {@link String name} of the Redis channel; must not be {@literal null}.
	 * @return the {@link ChannelTopic} for the given {@code channelName}.
	 * @since 3.5
	 */
	static ChannelTopic channel(String channelName) {
		return ChannelTopic.of(channelName);
	}

	/**
	 * Create a new {@link PatternTopic} for channel subscriptions based on a {@code pattern}.
	 *
	 * @param pattern {@link String pattern} used to match channels; must not be {@literal null} or empty.
	 * @return the {@link PatternTopic} for the given {@code pattern}.
	 * @since 3.5
	 */
	static PatternTopic pattern(String pattern) {
		return PatternTopic.of(pattern);
	}

	/**
	 * Returns the topic (as a String).
	 *
	 * @return the topic
	 */
	String getTopic();

}
