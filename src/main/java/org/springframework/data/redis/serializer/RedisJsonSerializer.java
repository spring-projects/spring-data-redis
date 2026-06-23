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

/**
 * {@link RedisSerializer} extension for converting Objects to and from their JSON {@code byte[]} representation. It is
 * recommended that implementations handle {@literal null} objects on serialization (by writing JSON {@code null}) and
 * empty/{@code null} input on deserialization (by returning {@literal null}). Note that Redis does not accept
 * {@literal null} keys or values but can return {@code null} replies for non-existing keys.
 * <p>
 * Beyond the {@code byte[]}-based {@link #serialize(Object) serialize} and {@link #deserialize(byte[], Class)
 * deserialize} methods inherited from {@link RedisSerializer}, this interface adds a
 * {@link ParameterizedTypeReference}-based variant to deserialize generic types.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public interface RedisJsonSerializer extends RedisSerializer<Object> {

	/**
	 * Deserialize the given {@code rawJson} into an instance of the type described by {@code typeRef}. Use this variant
	 * for generic types such as {@code List<Person>} that cannot be expressed as a {@link Class}.
	 *
	 * @param rawJson the JSON representation to read; can be {@literal null}.
	 * @param typeRef reference describing the target type; must not be {@literal null}.
	 * @param <T> the target type.
	 * @return the deserialized object, or {@literal null} if {@code rawJson} is empty or represents JSON {@code null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	<T> @Nullable T deserialize(byte @Nullable [] rawJson, ParameterizedTypeReference<T> typeRef)
			throws SerializationException;

}
