/*
 * Copyright 2015 the original author or authors.
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

import java.nio.charset.Charset;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.redis.util.ByteUtils;

/**
 * {@link RedisKeyExpiredEvent} is Redis specific {@link ApplicationEvent} published when a specific key in Redis
 * expires. It might but must not hold the expired value itself next to the key.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisKeyExpiredEvent<T> extends RedisKeyspaceEvent {

	private final byte[][] args;
	private final Object value;

	/**
	 * Creates new {@link RedisKeyExpiredEvent}.
	 * 
	 * @param key
	 */
	public RedisKeyExpiredEvent(byte[] key) {
		this(key, null);
	}

	/**
	 * Creates new {@link RedisKeyExpiredEvent}
	 * 
	 * @param key
	 * @param value
	 */
	public RedisKeyExpiredEvent(byte[] key, Object value) {
		super(key);

		args = ByteUtils.split(key, ':');
		this.value = value;
	}

	/**
	 * Gets the keyspace in which the expiration occured.
	 * 
	 * @return {@literal null} if it could not be determined.
	 */
	public String getKeyspace() {

		if (args.length >= 2) {
			return new String(args[0], Charset.forName("UTF-8"));
		}

		return null;
	}

	/**
	 * Get the expired objects id;
	 * 
	 * @return
	 */
	public byte[] getId() {
		return args.length == 2 ? args[1] : args[0];
	}

	/**
	 * Get the expired Object
	 * 
	 * @return {@literal null} if not present.
	 */
	public Object getValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.EventObject#toString()
	 */
	@Override
	public String toString() {
		return "RedisKeyExpiredEvent [keyspace=" + getKeyspace() + ", id=" + getId() + "]";
	}

}
