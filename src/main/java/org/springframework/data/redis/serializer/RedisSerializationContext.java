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
 * <p />
 * This context provides {@link SerializationPair}s for key, value, hash-key (field), hash-value and {@link String}
 * serialization and deserialization.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see RedisElementWriter
 * @see RedisElementReader
 */
public interface RedisSerializationContext<K, V> {

	/**
	 * Creates a new {@link RedisSerializationContextBuilder}.
	 *
	 * @param <K> expected key type.
	 * @param <V> expected value type.
	 * @return a new {@link RedisSerializationContextBuilder}.
	 */
	@SuppressWarnings("unchecked")
	static <K, V> RedisSerializationContextBuilder<K, V> newSerializationContext() {
		return new DefaultRedisSerializationContext.DefaultRedisSerializationContextBuilder();
	}

	/**
	 * Creates a new {@link RedisSerializationContextBuilder} using a given default {@link RedisSerializer}.
	 *
	 * @param defaultSerializer must not be {@literal null}.
	 * @param <K> expected key type.
	 * @param <V> expected value type.
	 * @return a new {@link RedisSerializationContextBuilder}.
	 */
	static <K, V> RedisSerializationContextBuilder<K, V> newSerializationContext(RedisSerializer<?> defaultSerializer) {

		Assert.notNull(defaultSerializer, "DefaultSerializer must not be null!");

		return newSerializationContext(SerializationPair.fromSerializer(defaultSerializer));
	}

	/**
	 * Creates a new {@link RedisSerializationContextBuilder} using a given default {@link SerializationPair}.
	 *
	 * @param serializationPair must not be {@literal null}.
	 * @param <K> expected key type.
	 * @param <V> expected value type.
	 * @return a new {@link RedisSerializationContextBuilder}.
	 */
	@SuppressWarnings("unchecked")
	static <K, V> RedisSerializationContextBuilder<K, V> newSerializationContext(SerializationPair<?> serializationPair) {

		Assert.notNull(serializationPair, "SerializationPair must not be null!");

		return new DefaultRedisSerializationContext.DefaultRedisSerializationContextBuilder() //
				.key(serializationPair).value(serializationPair) //
				.hashKey(serializationPair).hashValue(serializationPair);
	}

	/**
	 * Creates a new {@link RedisSerializationContext} using a {@link SerializationPair#raw()} serialization pair.
	 *
	 * @return
	 */
	static RedisSerializationContext<byte[], byte[]> raw() {
		return just(SerializationPair.raw());
	}

	/**
	 * Creates a new {@link RedisSerializationContext} using a {@link StringRedisSerializer}.
	 *
	 * @return
	 */
	static RedisSerializationContext<String, String> string() {
		return fromSerializer(StringRedisSerializer.UTF_8);
	}

	/**
	 * Creates a new {@link RedisSerializationContext} using the given {@link RedisSerializer}.
	 *
	 * @param serializer must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	static <T> RedisSerializationContext<T, T> fromSerializer(RedisSerializer<T> serializer) {
		return just(SerializationPair.fromSerializer(serializer));
	}

	/**
	 * Creates a new {@link RedisSerializationContext} using the given {@link SerializationPair}.
	 *
	 * @param serializationPair
	 * @param <T>
	 * @return
	 */
	static <T> RedisSerializationContext<T, T> just(SerializationPair<T> serializationPair) {
		return RedisSerializationContext.<T, T>newSerializationContext(serializationPair).build();
	}

	/**
	 * @return {@link SerializationPair} for key-typed serialization and deserialization.
	 */
	SerializationPair<K> getKeySerializationPair();

	/**
	 * @return {@link SerializationPair} for value-typed serialization and deserialization.
	 */
	SerializationPair<V> getValueSerializationPair();

	/**
	 * @return {@link SerializationPair} for hash-key-typed serialization and deserialization.
	 */
	<HK> SerializationPair<HK> getHashKeySerializationPair();

	/**
	 * @return {@link SerializationPair} for hash-value-typed serialization and deserialization.
	 */
	<HV> SerializationPair<HV> getHashValueSerializationPair();

	/**
	 * @return {@link SerializationPair} for {@link String}-typed serialization and deserialization.
	 */
	SerializationPair<String> getStringSerializationPair();

	/**
	 * Typed serialization tuple.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	interface SerializationPair<T> {

		/**
		 * Creates a {@link SerializationPair} adapter given {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return a {@link SerializationPair} adapter for {@link RedisSerializer}.
		 */
		static <T> SerializationPair<T> fromSerializer(RedisSerializer<T> serializer) {

			Assert.notNull(serializer, "RedisSerializer must not be null!");

			return new RedisSerializerToSerializationPairAdapter<>(serializer);
		}

		/**
		 * Creates a {@link SerializationPair} adapter given {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return a {@link SerializationPair} encapsulating {@link RedisElementReader} and {@link RedisElementWriter}.
		 */
		static <T> SerializationPair<T> just(RedisElementReader<? extends T> reader,
				RedisElementWriter<? extends T> writer) {

			Assert.notNull(reader, "RedisElementReader must not be null!");
			Assert.notNull(writer, "RedisElementWriter must not be null!");

			return new DefaultSerializationPair<>(reader, writer);
		}

		/**
		 * Creates a pass through {@link SerializationPair} to pass-thru {@link ByteBuffer} objects.
		 *
		 * @return a pass through {@link SerializationPair}.
		 */
		static <T> SerializationPair<T> raw() {
			return RedisSerializerToSerializationPairAdapter.raw();
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
	 * Builder for {@link RedisSerializationContext}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	interface RedisSerializationContextBuilder<K, V> {

		/**
		 * Set the key {@link SerializationPair}.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		RedisSerializationContextBuilder<K, V> key(SerializationPair<K> pair);

		/**
		 * Set the key {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> key(RedisElementReader<K> reader, RedisElementWriter<K> writer) {

			key(SerializationPair.just(reader, writer));
			return this;
		}

		/**
		 * Set the key {@link SerializationPair} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> key(RedisSerializer<K> serializer) {

			key(SerializationPair.fromSerializer(serializer));
			return this;
		}

		/**
		 * Set the value {@link SerializationPair}.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		RedisSerializationContextBuilder<K, V> value(SerializationPair<V> pair);

		/**
		 * Set the value {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> value(RedisElementReader<V> reader, RedisElementWriter<V> writer) {

			value(SerializationPair.just(reader, writer));
			return this;
		}

		/**
		 * Set the value {@link SerializationPair} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> value(RedisSerializer<V> serializer) {

			value(SerializationPair.fromSerializer(serializer));
			return this;
		}

		/**
		 * Set the hash key {@link SerializationPair}.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		RedisSerializationContextBuilder<K, V> hashKey(SerializationPair<?> pair);

		/**
		 * Set the hash key {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> hashKey(RedisElementReader<? extends Object> reader,
				RedisElementWriter<? extends Object> writer) {

			hashKey(SerializationPair.just(reader, writer));
			return this;
		}

		/**
		 * Set the hash key {@link SerializationPair} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> hashKey(RedisSerializer<? extends Object> serializer) {

			hashKey(SerializationPair.fromSerializer(serializer));
			return this;
		}

		/**
		 * Set the hash value {@link SerializationPair}.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		RedisSerializationContextBuilder<K, V> hashValue(SerializationPair<?> pair);

		/**
		 * Set the hash value {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> hashValue(RedisElementReader<? extends Object> reader,
				RedisElementWriter<? extends Object> writer) {

			hashValue(SerializationPair.just(reader, writer));
			return this;
		}

		/**
		 * Set the hash value {@link SerializationPair} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> hashValue(RedisSerializer<? extends Object> serializer) {

			hashValue(SerializationPair.fromSerializer(serializer));
			return this;
		}

		/**
		 * Set the string {@link SerializationPair}.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		RedisSerializationContextBuilder<K, V> string(SerializationPair<String> pair);

		/**
		 * Set the string {@link RedisElementReader} and {@link RedisElementWriter}.
		 *
		 * @param reader must not be {@literal null}.
		 * @param writer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> string(RedisElementReader<String> reader,
				RedisElementWriter<String> writer) {

			string(SerializationPair.just(reader, writer));
			return this;
		}

		/**
		 * Set the string {@link SerializationPair} given a {@link RedisSerializer}.
		 *
		 * @param serializer must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		default RedisSerializationContextBuilder<K, V> string(RedisSerializer<String> serializer) {

			string(SerializationPair.fromSerializer(serializer));
			return this;
		}

		/**
		 * Builds a {@link RedisSerializationContext}.
		 *
		 * @return the {@link RedisSerializationContext}.
		 */
		RedisSerializationContext<K, V> build();
	}
}
