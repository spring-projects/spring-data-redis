/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.context.ApplicationEvent;
import org.springframework.lang.Nullable;

/**
 * Redis specific {@link ApplicationEvent} published when a key expires in Redis.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisKeyspaceEvent extends ApplicationEvent {

	private final @Nullable String channel;

	/**
	 * Creates new {@link RedisKeyspaceEvent}.
	 *
	 * @param key The key that expired. Must not be {@literal null}.
	 */
	public RedisKeyspaceEvent(byte[] key) {
		this(null, key);
	}

	/**
	 * Creates new {@link RedisKeyspaceEvent}.
	 *
	 * @param channel The source channel aka subscription topic. Can be {@literal null}.
	 * @param key The key that expired. Must not be {@literal null}.
	 * @since 1.8
	 */
	public RedisKeyspaceEvent(@Nullable String channel, byte[] key) {

		super(key);
		this.channel = channel;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.EventObject#getSource()
	 */
	public byte[] getSource() {
		return (byte[]) super.getSource();
	}

	/**
	 * @return can be {@literal null}.
	 * @since 1.8
	 */
	@Nullable
	public String getChannel() {
		return this.channel;
	}

}
