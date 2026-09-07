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
 * {@link RedisSerializer} extension for converting Objects to and from their JSON {@code byte[]} representation.
 * <p>
 * A JSON serializer primarily adds convenient methods for deserializing JSON into typed objects.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @author Moritz Halbritter
 * @since 4.2
 */
public interface RedisJsonSerializer extends RedisSerializer<Object> {

	/**
	 * Deserialize the given {@code source} into an instance of the given {@code type}.
	 *
	 * @param source the JSON representation to read. Can be {@literal null}.
	 * @param type the target type.
	 * @param <T> the target type.
	 * @return the deserialized object, or {@literal null} if {@code source} is {@literal null} or empty, or represents
	 *         JSON {@literal null}.
	 * @throws SerializationException if the JSON cannot be deserialized.
	 */
	@Override
	@SuppressWarnings("unchecked")
	default <T> @Nullable T deserialize(byte @Nullable [] source, Class<T> type) throws SerializationException {

		// Overrides the RedisSerializer default, which answers with an untyped deserialize(byte[]) for every type this
		// serializer canSerialize - always the case here, as the target type is Object - and would therefore silently
		// ignore type. Routes through deserialize(byte[], ResolvableType) instead.
		return source == null ? null : (T) deserialize(source, ResolvableType.forClass(type));
	}

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

	/**
	 * Split a top-level JSON array into its immediate elements, as raw byte slices.
	 * <p>
	 * The default implementation round-trips {@code source} through {@link #deserialize} and {@link #serialize}, so the
	 * returned slices are re-serialized rather than cut out of {@code source}: number formatting and precision follow
	 * whatever this serializer maps JSON numbers to (e.g. {@code 1.10} may come back as {@code 1.1}). Override to slice
	 * {@code source} byte-exactly.
	 *
	 * @param source a JSON array.
	 * @return the immediate elements as raw byte slices, in order. Empty if the array is empty.
	 * @throws SerializationException if {@code source} is not a JSON array or cannot be read.
	 */
	default List<byte[]> splitArray(byte[] source) throws SerializationException {

		Object elements = deserialize(source, ResolvableType.forClassWithGenerics(List.class, Object.class));

		if (!(elements instanceof List<?> list)) {
			throw new SerializationException("Source is not a JSON array");
		}

		return list.stream().map(this::serializeElement).toList();
	}

	/**
	 * Split a top-level JSON object into its immediate members, as raw byte slices keyed by the (already unescaped)
	 * member name.
	 * <p>
	 * The default implementation re-serializes the member values, with the same caveat as {@link #splitArray}.
	 *
	 * @param source a JSON object.
	 * @return the immediate members as raw byte slices keyed by member name, in order. Empty if the object is empty.
	 * @throws SerializationException if {@code source} is not a JSON object or cannot be read.
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
	 * Assemble a JSON object from raw byte slices keyed by member name, the inverse of {@link #splitObject}. Member
	 * names are escaped as needed; each value must be a well-formed JSON value.
	 * <p>
	 * The default implementation round-trips the values through {@link #deserialize} and {@link #serialize}, with the
	 * same caveat as {@link #splitArray}. Override to write them verbatim.
	 *
	 * @param members the members to write, value as raw JSON byte slice, keyed by member name. Written in iteration
	 *          order.
	 * @return the JSON object. {@code {}} if {@code members} is empty.
	 * @throws SerializationException if the object cannot be written.
	 */
	default byte[] joinObject(Map<String, byte[]> members) throws SerializationException {

		Map<String, @Nullable Object> values = new LinkedHashMap<>();
		members.forEach((name, value) -> values.put(name, deserialize(value, ResolvableType.forClass(Object.class))));

		return serialize(values);
	}

	/**
	 * Serialize a single value split out of a JSON container. {@link #serialize} maps {@literal null} to an empty array
	 * rather than to the JSON literal {@code null}, which would lose the distinction between a JSON {@literal null} and
	 * an absent value.
	 */
	private byte[] serializeElement(@Nullable Object value) {
		return value == null ? "null".getBytes(StandardCharsets.UTF_8) : serialize(value);
	}

}
