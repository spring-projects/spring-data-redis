/*
 * Copyright 2017-2025 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RedisSerializationContext}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Shyngys Sapraliyev
 * @author John Blum
 * @since 2.0
 */
class DefaultRedisSerializationContext<K, V> implements RedisSerializationContext<K, V> {

	private final SerializationPair<K> keyTuple;
	private final SerializationPair<V> valueTuple;
	private final SerializationPair<?> hashKeyTuple;
	private final SerializationPair<?> hashValueTuple;
	private final SerializationPair<String> stringTuple;

	private DefaultRedisSerializationContext(SerializationPair<K> keyTuple, SerializationPair<V> valueTuple,
			SerializationPair<?> hashKeyTuple, SerializationPair<?> hashValueTuple, SerializationPair<String> stringTuple) {

		this.keyTuple = keyTuple;
		this.valueTuple = valueTuple;
		this.hashKeyTuple = hashKeyTuple;
		this.hashValueTuple = hashValueTuple;
		this.stringTuple = stringTuple;
	}

	@Override
	public SerializationPair<K> getKeySerializationPair() {
		return this.keyTuple;
	}

	@Override
	public SerializationPair<V> getValueSerializationPair() {
		return this.valueTuple;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <HK> SerializationPair<HK> getHashKeySerializationPair() {
		return (SerializationPair<HK>) this.hashKeyTuple;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <HV> SerializationPair<HV> getHashValueSerializationPair() {
		return (SerializationPair<HV>) this.hashValueTuple;
	}

	@Override
	public SerializationPair<String> getStringSerializationPair() {
		return this.stringTuple;
	}

	/**
	 * Default implementation of {@link RedisSerializationContextBuilder}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @author Zhou KQ
	 * @since 2.0
	 */
	static class DefaultRedisSerializationContextBuilder<K, V> implements RedisSerializationContextBuilder<K, V> {

		private @Nullable SerializationPair<K> keyTuple;
		private @Nullable SerializationPair<V> valueTuple;
		private @Nullable SerializationPair<?> hashKeyTuple;
		private @Nullable SerializationPair<?> hashValueTuple;

		private SerializationPair<String> stringTuple = SerializationPair.fromSerializer(RedisSerializer.string());

		@Override
		public RedisSerializationContextBuilder<K, V> key(SerializationPair<K> tuple) {

			Assert.notNull(tuple, "SerializationPair must not be null");

			this.keyTuple = tuple;

			return this;
		}

		@Override
		public RedisSerializationContextBuilder<K, V> value(SerializationPair<V> tuple) {

			Assert.notNull(tuple, "SerializationPair must not be null");

			this.valueTuple = tuple;

			return this;
		}

		@Override
		public RedisSerializationContextBuilder<K, V> hashKey(SerializationPair<?> tuple) {

			Assert.notNull(tuple, "SerializationPair must not be null");

			this.hashKeyTuple = tuple;

			return this;
		}

		@Override
		public RedisSerializationContextBuilder<K, V> hashValue(SerializationPair<?> tuple) {

			Assert.notNull(tuple, "SerializationPair must not be null");

			this.hashValueTuple = tuple;

			return this;
		}

		@Override
		public RedisSerializationContextBuilder<K, V> string(SerializationPair<String> tuple) {

			Assert.notNull(tuple, "SerializationPair must not be null");

			this.stringTuple = tuple;

			return this;
		}

		@Override
		public RedisSerializationContext<K, V> build() {

			Assert.notNull(this.keyTuple, "Key SerializationPair must not be null");
			Assert.notNull(this.valueTuple, "Value SerializationPair must not be null");
			Assert.notNull(this.hashKeyTuple, "HashKey SerializationPair must not be null");
			Assert.notNull(this.hashValueTuple, "HashValue SerializationPair must not be null");

			return new DefaultRedisSerializationContext<>(this.keyTuple, this.valueTuple, this.hashKeyTuple,
					this.hashValueTuple, this.stringTuple);
		}
	}
}
