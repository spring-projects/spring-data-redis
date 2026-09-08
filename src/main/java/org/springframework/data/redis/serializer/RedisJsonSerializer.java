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

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;

/**
 * {@link RedisSerializer} extension for converting between Java objects and JSON bytes.
 * <p>
 * Provides deserialization into specific target types, including generic types described by
 * {@link ParameterizedTypeReference} or {@link ResolvableType}. Also supports extracting array elements and object
 * members as JSON and combining members into a JSON object.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @author Moritz Halbritter
 * @since 4.2
 */
public interface RedisJsonSerializer extends RedisSerializer<Object> {

	/**
	 * Deserialize the given {@code source} into an instance of the type described by a
	 * {@link ParameterizedTypeReference}.
	 * <p>
	 * Use this variant for generic types such as {@code List<Person>} that cannot be expressed as a {@link Class}.
	 *
	 * @param source the JSON bytes to read.
	 * @param typeRef the reference describing the target type.
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
	 * Deserialize the given {@code source} into an instance of the type described by {@link ResolvableType}.
	 * <p>
	 * Use this variant for generic types such as {@code List<Person>} that cannot be expressed as a {@link Class}.
	 *
	 * @param source the JSON bytes to read.
	 * @param type the target type.
	 * @return the deserialized object, or {@literal null} if {@code source} is empty or represents JSON {@literal null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	@Nullable
	Object deserialize(byte[] source, ResolvableType type) throws SerializationException;

	/**
	 * Return a {@link Splitter} for this serializer.
	 */
	default Splitter splitter() {
		return new DefaultSplitter(this);
	}

	/**
	 * Splitter for JSON arrays and objects.
	 *
	 * @author Moritz Halbritter
	 */
	interface Splitter extends RedisJsonSerializer {

		/**
		 * Extract immediate elements of a JSON array as individual JSON values.
		 * <p>
		 * Note: The default implementation deserializes the array and serializes each element separately. This may change
		 * number formatting or precision. For example, {@code 1.10} may become {@code 1.1}. Implementations should override
		 * this method to preserve the original bytes of each element.
		 *
		 * @param source the JSON array to read.
		 * @return the JSON bytes for each element in array order, or an empty list if the array is empty.
		 * @throws SerializationException if the source is not a JSON array or cannot be read.
		 */
		default List<byte[]> splitArray(byte[] source) throws SerializationException {

			Object elements = deserialize(source, ResolvableType.forClassWithGenerics(List.class, Object.class));

			if (!(elements instanceof List<?> list)) {
				throw new SerializationException("Source is not a JSON array");
			}

			return list.stream().map(this::serializeElement).toList();
		}

		/**
		 * Extract the immediate members of a JSON object as individual JSON values.
		 * <p>
		 * Note: Map keys contain the unescaped member names. Values contain the JSON bytes for each member. The default
		 * implementation deserializes the object and serializes each member value separately. See
		 * {@link #splitArray(byte[])} for the effect on number formatting and precision.
		 *
		 * @param source the JSON object to read.
		 * @return the members in source order, or an empty map if the object is empty.
		 * @throws SerializationException if the source is not a JSON object or cannot be read.
		 */
		default Map<String, byte[]> splitObject(byte[] source) throws SerializationException {

			Object members = deserialize(source, ResolvableType.forClassWithGenerics(Map.class,
					ResolvableType.forClass(String.class), ResolvableType.forClass(Object.class)));

			if (!(members instanceof Map<?, ?> map)) {
				throw new SerializationException("Source is not a JSON object");
			}

			Map<String, byte[]> result = new LinkedHashMap<>();
			map.forEach((name, value) -> result.put(name.toString(), serializeElement(value)));
			return result;
		}

		/**
		 * Create a JSON object from the given members.
		 * <p>
		 * Map keys supply the member names, which are escaped as needed. Each value must contain a valid JSON value.
		 * Members are written in map iteration order.
		 * <p>
		 * Note: The default implementation deserializes the member values and serializes the resulting object. See
		 * {@link #splitArray(byte[])} for the effect on number formatting and precision. Implementations can override this
		 * method to preserve the supplied JSON bytes for each value.
		 *
		 * @param members the member names and their JSON bytes.
		 * @return the JSON object as bytes, or the representation of {@code {}} if the map is empty.
		 * @throws SerializationException if the object cannot be written.
		 * @see #splitObject(byte[])
		 */
		default byte[] joinObject(Map<String, byte[]> members) throws SerializationException {

			Map<String, @Nullable Object> values = new LinkedHashMap<>();
			members.forEach((name, value) -> values.put(name, deserialize(value, ResolvableType.forClass(Object.class))));
			return serialize(values);
		}

		/**
		 * Serialize an array element or object member, preserving JSON {@literal null}.
		 * <p>
		 * A {@literal null} value is represented by the JSON literal {@code null} rather than the empty byte array used by
		 * {@link #serialize(Object)} for an absent value.
		 */
		private byte[] serializeElement(@Nullable Object value) {
			return value == null ? "null".getBytes(StandardCharsets.UTF_8) : serialize(value);
		}

	}

}
