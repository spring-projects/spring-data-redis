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

import org.springframework.data.redis.util.ByteUtils;

/**
 * @author Christoph Strobl
 */
public class RedisKeyExpiredEvent extends RedisKeyspaceEvent {

	private final byte[][] args;
	private final Object value;

	public RedisKeyExpiredEvent(byte[] key) {
		this(key, null);
	}

	public RedisKeyExpiredEvent(byte[] key, Object value) {
		super(key);

		args = ByteUtils.split(key, ':');
		this.value = value;
	}

	public String getKeyspace() {

		if (args.length == 2) {
			return new String(args[0], Charset.forName("UTF-8"));
		}

		return null;
	}

	public byte[] getId() {
		return args.length == 2 ? args[1] : args[0];
	}

	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "RedisKeyExpiredEvent [keyspace=" + getKeyspace() + ", id=" + getId() + "]";
	}

}
