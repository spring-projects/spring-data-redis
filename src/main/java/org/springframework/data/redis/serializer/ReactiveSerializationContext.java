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

import org.springframework.util.Assert;

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
	 * Creates a new {@link ReactiveSerializationContextBuilder}.
	 *
	 * @param <K> expected key type.
	 * @param <V> expected value type.
	 * @return a new {@link ReactiveSerializationContextBuilder}.
	 */
	static <K, V> ReactiveSerializationContextBuilder<K, V> builder() {
		return new DefaultReactiveSerializationContextBuilder<>();
	}

	/**
	 * @return {@link SerializationTuple} for key-typed serialization and deserialization.
	 */
	SerializationTuple<K> key();

	/**
	 * @return {@link SerializationTuple} for value-typed serialization and deserialization.
	 */
	SerializationTuple<V> value();

	/**
	 * @return {@link SerializationTuple} for hash-key-typed serialization and deserialization.
	 */
	<HK> SerializationTuple<HK> hashKey();

	/**
	 * @return {@link SerializationTuple} for hash-value-typed serialization and deserialization.
	 */
	<HV> SerializationTuple<HV> hashValue();

	/**
	 * @return {@link SerializationTuple} for {@link String}-typed serialization and deserialization.
	 */
	SerializationTuple<String> string();

	/**
	 * Typed serialization tuple.
	 */
	interface SerializationTuple<T> {

		/**
		 * Creates a {@link SerializationTuple} adapter given {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return a {@link SerializationTuple} adapter for {@link RedisSerializer}.
		 */
		static <T> SerializationTuple<T> fromSerializer(RedisSerializer<T> serializer) {

			Assert.notNull(serializer, "RedisSerializer must not be null!");

			return new RedisSerializerTupleAdapter<T>(serializer);
		}

		/**
		 * Creates a {@link SerializationTuple} adapter given {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return a {@link SerializationTuple} encapsulating {@link RedisElementReader} and {@link RedisElementWriter}.
		 */
		static <T> SerializationTuple<T> just(RedisElementReader<? extends T> reader,
				RedisElementWriter<? extends T> writer) {

			Assert.notNull(reader, "RedisElementReader must not be null!");
			Assert.notNull(writer, "RedisElementWriter must not be null!");

			return new DefaultSerializationTuple<>(reader, writer);
		}

		/**
		 * Creates a pass-thru {@link SerializationTuple} to pass-thru {@link ByteBuffer} objects.
		 *
		 * @return a pass-thru {@link SerializationTuple}.
		 */
		static <T> SerializationTuple<T> raw() {
			return RedisSerializerTupleAdapter.raw();
		}

		/**
		 * @return the {@link RedisElementReader}.
		 */
		RedisElementReader<T> getReader();

		/**
		 * Deserialize a {@link ByteBuffer} into the according type.
		 *
		 * @param buffer must not be {@literal null}.
		 * @return the deserialized value.
		 */
		default T read(ByteBuffer buffer) {
			return getReader().read(buffer);
		}

		/**
		 * @return the {@link RedisElementWriter}.
		 */
		RedisElementWriter<T> getWriter();

		/**
		 * Serialize a {@code element} to its {@link ByteBuffer} representation.
		 *
		 * @param element
		 * @return the {@link ByteBuffer} representing {@code element} in its binary form.
		 */
		default ByteBuffer write(T element) {
			return getWriter().write(element);
		}
	}

	/**
	 * Builder for {@link ReactiveSerializationContext}.
	 */
	interface ReactiveSerializationContextBuilder<K, V> {

		/**
		 * Set the key {@link SerializationTuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> key(SerializationTuple<K> tuple);

		/**
		 * Set the key {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> key(RedisElementReader<K> reader, RedisElementWriter<K> writer);

		/**
		 * Set the key {@link SerializationTuple} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> key(RedisSerializer<K> serializer);

		/**
		 * Set the value {@link SerializationTuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> value(SerializationTuple<V> tuple);

		/**
		 * Set the value {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> value(RedisElementReader<V> reader, RedisElementWriter<V> writer);

		/**
		 * Set the value {@link SerializationTuple} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> value(RedisSerializer<V> serializer);

		/**
		 * Set the hash key {@link SerializationTuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashKey(SerializationTuple<?> tuple);

		/**
		 * Set the hash key {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashKey(RedisElementReader<? extends Object> reader,
				RedisElementWriter<? extends Object> writer);

		/**
		 * Set the hash key {@link SerializationTuple} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashKey(RedisSerializer<? extends Object> serializer);

		/**
		 * Set the hash value {@link SerializationTuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashValue(SerializationTuple<?> tuple);

		/**
		 * Set the hash value {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashValue(RedisElementReader<? extends Object> reader,
				RedisElementWriter<? extends Object> writer);

		/**
		 * Set the hash value {@link SerializationTuple} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> hashValue(RedisSerializer<? extends Object> serializer);

		/**
		 * Set the string {@link SerializationTuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> string(SerializationTuple<String> tuple);

		/**
		 * Set the string {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> string(RedisElementReader<String> reader,
				RedisElementWriter<String> writer);

		/**
		 * Set the string {@link SerializationTuple} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		ReactiveSerializationContextBuilder<K, V> string(RedisSerializer<String> serializer);

		/**
		 * Builds a {@link ReactiveSerializationContext}.
		 *
		 * @return the {@link ReactiveSerializationContext}.
		 */
		ReactiveSerializationContext<K, V> build();
	}
}
