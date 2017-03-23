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

import org.springframework.data.redis.serializer.ReactiveSerializationContext.ReactiveSerializationContextBuilder;
import org.springframework.data.redis.serializer.ReactiveSerializationContext.SerializationPair;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveSerializationContextBuilder}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class DefaultReactiveSerializationContextBuilder<K, V> implements ReactiveSerializationContextBuilder<K, V> {

	private SerializationPair<K> keyTuple;
	private SerializationPair<V> valueTuple;
	private SerializationPair<?> hashKeyTuple;
	private SerializationPair<?> hashValueTuple;
	private SerializationPair<String> stringTuple = SerializationPair.fromSerializer(new StringRedisSerializer());

	@Override
	public ReactiveSerializationContextBuilder<K, V> key(SerializationPair<K> tuple) {

		Assert.notNull(tuple, "SerializationPair must not be null!");

		this.keyTuple = tuple;
		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> value(SerializationPair<V> tuple) {

		Assert.notNull(tuple, "SerializationPair must not be null!");

		this.valueTuple = tuple;
		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashKey(SerializationPair<?> tuple) {

		Assert.notNull(tuple, "SerializationPair must not be null!");

		this.hashKeyTuple = tuple;
		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashValue(SerializationPair<?> tuple) {

		Assert.notNull(tuple, "SerializationPair must not be null!");

		this.hashValueTuple = tuple;
		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> string(SerializationPair<String> tuple) {

		Assert.notNull(tuple, "SerializationPair must not be null!");

		this.hashValueTuple = tuple;
		return this;
	}

	@Override
	public ReactiveSerializationContext<K, V> build() {

		Assert.notNull(keyTuple, "Key SerializationPair must not be null!");
		Assert.notNull(valueTuple, "Value SerializationPair must not be null!");
		Assert.notNull(hashKeyTuple, "HashKey SerializationPair must not be null!");
		Assert.notNull(hashValueTuple, "ValueKey SerializationPair must not be null!");

		return new DefaultReactiveSerializationContext<K, V>(keyTuple, valueTuple, hashKeyTuple, hashValueTuple,
				stringTuple);
	}

	static class DefaultReactiveSerializationContext<K, V> implements ReactiveSerializationContext<K, V> {

		private final SerializationPair<K> keyTuple;
		private final SerializationPair<V> valueTuple;
		private final SerializationPair<?> hashKeyTuple;
		private final SerializationPair<?> hashValueTuple;
		private final SerializationPair<String> stringTuple;

		public DefaultReactiveSerializationContext(SerializationPair<K> keyTuple, SerializationPair<V> valueTuple,
				SerializationPair<?> hashKeyTuple, SerializationPair<?> hashValueTuple, SerializationPair<String> stringTuple) {

			this.keyTuple = keyTuple;
			this.valueTuple = valueTuple;
			this.hashKeyTuple = hashKeyTuple;
			this.hashValueTuple = hashValueTuple;
			this.stringTuple = stringTuple;
		}

		@Override
		public SerializationPair<K> getKeySerializationPair() {
			return keyTuple;
		}

		@Override
		public SerializationPair<V> getValueSerializationPair() {
			return valueTuple;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <HK> SerializationPair<HK> getHashKeySerializationPair() {
			return (SerializationPair) hashKeyTuple;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <HV> SerializationPair<HV> getHashValueSerializationPair() {
			return (SerializationPair) hashValueTuple;
		}

		@Override
		public SerializationPair<String> getStringSerializationPair() {
			return stringTuple;
		}
	}
}
