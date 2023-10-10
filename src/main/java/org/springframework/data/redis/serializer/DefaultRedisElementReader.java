/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.nio.ByteBuffer;

import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link RedisElementReader}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class DefaultRedisElementReader<T> implements RedisElementReader<T> {

	private final @Nullable RedisSerializer<T> serializer;

	DefaultRedisElementReader(@Nullable RedisSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T read(ByteBuffer buffer) {

		if (serializer == null) {
			return (T) buffer;
		}

		return serializer.deserialize(ByteUtils.getBytes(buffer));
	}

}
