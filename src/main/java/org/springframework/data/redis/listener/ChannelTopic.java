/*
 * Copyright 2011-2024 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * {@link Topic Channel Topic} implementation mapping to a Redis channel.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author John Blum
 */
public class ChannelTopic extends AbstractTopic {

	/**
	 * Constructs a new {@link ChannelTopic} instance.
	 *
	 * @param channelName must not be {@literal null}.
	 */
	public ChannelTopic(String channelName) {

		super(channelName);
		Assert.notNull(channelName, "Channel name must not be null");
	}

	/**
	 * Create a new {@link ChannelTopic} for channel subscriptions.
	 *
	 * @param channelName {@link String name} of the Redis channel; must not be {@literal null}.
	 * @return the {@link ChannelTopic} for the given {@code channelName}.
	 * @since 2.1
	 */
	public static ChannelTopic of(String channelName) {
		return new ChannelTopic(channelName);
	}
}
