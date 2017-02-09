/*
 * Copyright 2017 the original author or authors.
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

import java.nio.ByteBuffer;

/**
 * Serialization context for reactive use.
 * <p>
 * This context provides {@link SerializationTuple}s for key, value, hash-key (field), hash-value and {@link String}
 * serialization and deserialization.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see RedisElementWriter
 * @see RedisElementReader
 */
public interface ReactiveSerializationContext<K, V> {

	/**
	 * @return {@link SerializationTuple} for key-typed serialization and deserialization.
	 */
	SerializationTuple<K> key();

	/**
	 * @return {@link SerializationTuple} for value-typed serialization and deserialization.
	 */
	SerializationTuple<V> value();

	/**
	 * @return {@link SerializationTuple} for {@link String}-typed serialization and deserialization.
	 */
	SerializationTuple<String> string();

	/**
	 * @return {@link SerializationTuple} for hash-key-typed serialization and deserialization.
	 */
	<HK> SerializationTuple<HK> hashKey();

	/**
	 * @return {@link SerializationTuple} for hash-value-typed serialization and deserialization.
	 */
	<HV> SerializationTuple<HV> hashValue();

	/**
	 * Typed serialization tuple.
	 *
	 * @param <T>
	 */
	interface SerializationTuple<T> {

		/**
		 * @return the {@link RedisElementReader}.
		 */
		RedisElementReader<T> reader();

		/**
		 * Deserialize a {@link ByteBuffer} into the according type.
		 *
		 * @param buffer must not be {@literal null}.
		 * @return the deserialized value.
		 */
		default T read(ByteBuffer buffer) {
			return reader().read(buffer);
		}

		/**
		 * @return the {@link RedisElementWriter}.
		 */
		RedisElementWriter<T> writer();

		/**
		 * Serialize a {@code element} to its {@link ByteBuffer} representation.
		 *
		 * @param element
		 * @return the {@link ByteBuffer} representing {@code element} in its binary form.
		 */
		default ByteBuffer write(T element) {
			return writer().write(element);
		}
	}
}
