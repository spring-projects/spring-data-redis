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
 * Serializer for converting Objects to and from their JSON {@link String} representation. It is recommended that
 * implementations handle {@literal null} objects on serialization (by writing JSON {@code null}) and JSON {@code null}
 * on deserialization (by returning {@literal null}). Note that Redis does not accept {@literal null} keys or values but
 * can return {@code null} replies for non-existing keys.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public interface RedisJsonSerializer {

	/**
	 * Serialize the given {@code value} to its JSON representation.
	 *
	 * @param value the object to serialize; may be {@literal null}, in which case JSON {@code null} is written.
	 * @return the JSON representation, never {@literal null}.
	 * @throws SerializationException if the value cannot be serialized.
	 */
	String serialize(@Nullable Object value) throws SerializationException;

	/**
	 * Deserialize the given {@code rawJson} into an instance of {@code type}.
	 *
	 * @param rawJson the JSON representation to read; must not be {@literal null}.
	 * @param type the target type; must not be {@literal null}.
	 * @param <T> the target type.
	 * @return the deserialized object, or {@literal null} if {@code rawJson} represents JSON {@code null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	<T> T deserialize(String rawJson, Class<T> type) throws SerializationException;

	/**
	 * Deserialize the given {@code rawJson} into an instance of the type described by {@code typeRef}. Use this variant
	 * for generic types such as {@code List<Person>} that cannot be expressed as a {@link Class}.
	 *
	 * @param rawJson the JSON representation to read; must not be {@literal null}.
	 * @param typeRef reference describing the target type; must not be {@literal null}.
	 * @param <T> the target type.
	 * @return the deserialized object, or {@literal null} if {@code rawJson} represents JSON {@code null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	<T> T deserialize(String rawJson, ParameterizedTypeReference<T> typeRef) throws SerializationException;

}
