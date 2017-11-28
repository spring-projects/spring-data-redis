/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.listener;

import lombok.EqualsAndHashCode;

import org.springframework.util.Assert;

/**
 * Channel topic implementation (maps to a Redis channel).
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
@EqualsAndHashCode
public class ChannelTopic implements Topic {

	private final String channelName;

	/**
	 * Constructs a new {@link ChannelTopic} instance.
	 *
	 * @param name must not be {@literal null}.
	 */
	public ChannelTopic(String name) {

		Assert.notNull(name, "Topic name must not be null!");

		this.channelName = name;
	}

	/**
	 * Create a new {@link ChannelTopic} for channel subscriptions.
	 *
	 * @param name the channel name, must not be {@literal null} or empty.
	 * @return the {@link ChannelTopic} for {@code channelName}.
	 * @since 2.1
	 */
	public static ChannelTopic of(String name) {
		return new ChannelTopic(name);
	}

	/**
	 * @return topic name.
	 */
	@Override
	public String getTopic() {
		return channelName;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return channelName;
	}
}
