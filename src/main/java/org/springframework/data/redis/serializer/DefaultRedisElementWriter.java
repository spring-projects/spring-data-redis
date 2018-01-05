/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.serializer;

import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link RedisElementWriter}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor
class DefaultRedisElementWriter<T> implements RedisElementWriter<T> {

	private final @Nullable RedisSerializer<T> serializer;

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisElementWriter#write(java.lang.Object)
	 */
	@Override
	public ByteBuffer write(T value) {

		if (serializer != null) {
			return ByteBuffer.wrap(serializer.serialize(value));
		}

		if (value instanceof byte[]) {
			return ByteBuffer.wrap((byte[]) value);
		}

		if (value instanceof ByteBuffer) {
			return (ByteBuffer) value;
		}

		throw new IllegalStateException("Cannot serialize value without a serializer");

	}
}
