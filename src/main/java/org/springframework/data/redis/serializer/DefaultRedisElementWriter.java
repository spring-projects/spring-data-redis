/*
 * Copyright 2017-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Default implementation of {@link RedisElementWriter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class DefaultRedisElementWriter<T> implements RedisElementWriter<T> {

	private final @Nullable RedisSerializer<T> serializer;

	DefaultRedisElementWriter(@Nullable RedisSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public ByteBuffer write(@Nullable T value) {

		if (serializer != null && (value == null || serializer.canSerialize(value.getClass()))) {
			byte[] serializedValue = serializer.serialize(value);
			return serializedValue != null ? ByteBuffer.wrap(serializedValue) : ByteBuffer.wrap(new byte[0]);
		}

		if (value instanceof byte[]) {
			return ByteBuffer.wrap((byte[]) value);
		}

		if (value instanceof ByteBuffer) {
			return (ByteBuffer) value;
		}

		throw new IllegalStateException(
				"Cannot serialize value of type %s without a serializer".formatted(ObjectUtils.nullSafeClassName(value)));
	}
}
