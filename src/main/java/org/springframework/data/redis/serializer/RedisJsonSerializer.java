/*
 * Copyright 2026-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;

/**
 * {@link RedisSerializer} extension for converting Objects to and from their JSON {@code byte[]} representation.
 * <p>
 * A JSON serializer primarily adds convenient methods for deserializing JSON into typed objects.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 */
public interface RedisJsonSerializer extends RedisSerializer<Object> {

	/**
	 * Deserialize the given {@code source} into an instance of the type described by a
	 * {@link ParameterizedTypeReference}. Use this variant for generic types such as {@code List<Person>} that cannot be
	 * expressed as a {@link Class}.
	 *
	 * @param source the JSON representation to read.
	 * @param typeRef reference describing the target type.
	 * @param <T> the target type.
	 * @return the deserialized object, or {@literal null} if {@code source} is empty or represents JSON {@literal null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	@SuppressWarnings("unchecked")
	default <T> @Nullable T deserialize(byte[] source, ParameterizedTypeReference<T> typeRef)
			throws SerializationException {
		return (T) deserialize(source, ResolvableType.forType(typeRef));
	}

	/**
	 * Deserialize the given {@code source} into an instance of the type described by {@link ResolvableType}. Use this
	 * variant for generic types such as {@code List<Person>} that cannot be expressed as a {@link Class}.
	 *
	 * @param source the JSON representation to read.
	 * @param type reference describing the target type.
	 * @return the deserialized object, or {@literal null} if {@code source} is empty or represents JSON {@literal null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	@Nullable
	Object deserialize(byte[] source, ResolvableType type) throws SerializationException;

}
