/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;

/**
 * Utility methods for stream serialization.
 * 
 * @author Mark Paluch
 * @since 2.2
 */
class StreamSerialization {

	/**
	 * Serialize the {@code value} using the optional {@link RedisSerializer}. If no conversion is possible, {@code value}
	 * is assumed to be a byte array.
	 * 
	 * @param serializer the serializer. Can be {@literal null}.
	 * @param value the value to serialize.
	 * @return the serialized (binary) representation of {@code value}.
	 */
	@SuppressWarnings("unchecked")
	static byte[] serialize(@Nullable RedisSerializer<?> serializer, Object value) {
		return canSerialize(serializer, value) ? ((RedisSerializer) serializer).serialize(value) : (byte[]) value;
	}

	/**
	 * Returns whether the given {@link RedisSerializer} is capable of serializing the {@code value} to {@literal byte[]}.
	 *
	 * @param serializer the serializer. Can be {@literal null}.
	 * @param value the value to serialize.
	 * @return {@literal true} if the given {@link RedisSerializer} is capable of serializing the {@code value} to
	 *         {@literal byte[]}.
	 */
	private static boolean canSerialize(@Nullable RedisSerializer<?> serializer, Object value) {
		return serializer != null && (value == null || serializer.canSerialize(value.getClass()));
	}
}
