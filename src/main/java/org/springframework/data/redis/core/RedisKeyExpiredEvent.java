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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.redis.core.convert.MappingRedisConverter.BinaryKeyspaceIdentifier;
import org.springframework.lang.Nullable;

/**
 * {@link RedisKeyExpiredEvent} is Redis specific {@link ApplicationEvent} published when a specific key in Redis
 * expires. It might but must not hold the expired value itself next to the key.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisKeyExpiredEvent<T> extends RedisKeyspaceEvent {

	/**
	 * Use {@literal UTF-8} as default charset.
	 */
	static final Charset CHARSET = StandardCharsets.UTF_8;

	private final BinaryKeyspaceIdentifier objectId;
	private final @Nullable Object value;

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
	public RedisKeyExpiredEvent(byte[] key, @Nullable Object value) {
		this(null, key, value);
	}

	/**
	 * Creates new {@link RedisKeyExpiredEvent}
	 *
	 * @pamam channel
	 * @param key
	 * @param value
	 * @since 1.8
	 */
	public RedisKeyExpiredEvent(@Nullable String channel, byte[] key, @Nullable Object value) {
		super(channel, key);

		if (BinaryKeyspaceIdentifier.isValid(key)) {
			this.objectId = BinaryKeyspaceIdentifier.of(key);
		} else {
			this.objectId = null;
		}

		this.value = value;
	}

	/**
	 * Gets the keyspace in which the expiration occured.
	 *
	 * @return {@literal null} if it could not be determined.
	 */
	public String getKeyspace() {
		return objectId != null ? new String(objectId.getKeyspace(), CHARSET) : null;
	}

	/**
	 * Get the expired objects id;
	 *
	 * @return
	 */
	public byte[] getId() {
		return objectId != null ? objectId.getId() : getSource();
	}

	/**
	 * Get the expired Object
	 *
	 * @return {@literal null} if not present.
	 */
	@Nullable
	public Object getValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.EventObject#toString()
	 */
	@Override
	public String toString() {

		byte[] id = getId();
		return "RedisKeyExpiredEvent [keyspace=" + getKeyspace() + ", id=" + (id == null ? null : new String(id)) + "]";
	}

}
