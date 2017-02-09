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
import org.springframework.data.redis.serializer.ReactiveSerializationContext.SerializationTuple;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveSerializationContextBuilder}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultReactiveSerializationContextBuilder<K, V> implements ReactiveSerializationContextBuilder<K, V> {

	private SerializationTuple<K> keyTuple;

	private SerializationTuple<V> valueTuple;

	private SerializationTuple<?> hashKeyTuple;

	private SerializationTuple<?> hashValueTuple;

	private SerializationTuple<String> stringTuple = SerializationTuple.fromSerializer(new StringRedisSerializer());

	@Override
	public ReactiveSerializationContextBuilder<K, V> key(SerializationTuple<K> tuple) {

		Assert.notNull(tuple, "SerializationTuple must not be null!");

		this.keyTuple = tuple;

		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> key(RedisElementReader<K> reader, RedisElementWriter<K> writer) {
		return key(SerializationTuple.just(reader, writer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> key(RedisSerializer<K> serializer) {
		return key(SerializationTuple.fromSerializer(serializer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> value(SerializationTuple<V> tuple) {

		Assert.notNull(tuple, "SerializationTuple must not be null!");

		this.valueTuple = tuple;

		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> value(RedisElementReader<V> reader, RedisElementWriter<V> writer) {
		return value(SerializationTuple.just(reader, writer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> value(RedisSerializer<V> serializer) {
		return value(SerializationTuple.fromSerializer(serializer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashKey(SerializationTuple<?> tuple) {

		Assert.notNull(tuple, "SerializationTuple must not be null!");

		this.hashKeyTuple = tuple;

		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashKey(RedisElementReader<?> reader, RedisElementWriter<?> writer) {
		return hashKey(SerializationTuple.just(reader, writer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashKey(RedisSerializer<?> serializer) {
		return hashKey(SerializationTuple.fromSerializer(serializer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashValue(SerializationTuple<?> tuple) {

		Assert.notNull(tuple, "SerializationTuple must not be null!");

		this.hashValueTuple = tuple;

		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashValue(RedisElementReader<?> reader,
			RedisElementWriter<?> writer) {
		return hashValue(SerializationTuple.just(reader, writer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> hashValue(RedisSerializer<?> serializer) {
		return hashValue(SerializationTuple.fromSerializer(serializer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> string(SerializationTuple<String> tuple) {

		Assert.notNull(tuple, "SerializationTuple must not be null!");

		this.hashValueTuple = tuple;

		return this;
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> string(RedisElementReader<String> reader,
			RedisElementWriter<String> writer) {
		return string(SerializationTuple.just(reader, writer));
	}

	@Override
	public ReactiveSerializationContextBuilder<K, V> string(RedisSerializer<String> serializer) {
		return string(SerializationTuple.fromSerializer(serializer));
	}

	@Override
	public ReactiveSerializationContext<K, V> build() {

		Assert.notNull(keyTuple, "Key SerializationTuple must not be null!");
		Assert.notNull(valueTuple, "Value SerializationTuple must not be null!");
		Assert.notNull(hashKeyTuple, "HashKey SerializationTuple must not be null!");
		Assert.notNull(hashValueTuple, "ValueKey SerializationTuple must not be null!");

		return new DefaultReactiveSerializationContext<K, V>(keyTuple, valueTuple, hashKeyTuple, hashValueTuple,
				stringTuple);
	}

	static class DefaultReactiveSerializationContext<K, V> implements ReactiveSerializationContext<K, V> {

		private final SerializationTuple<K> keyTuple;

		private final SerializationTuple<V> valueTuple;

		private final SerializationTuple<?> hashKeyTuple;

		private final SerializationTuple<?> hashValueTuple;

		private final SerializationTuple<String> stringTuple;

		public DefaultReactiveSerializationContext(SerializationTuple<K> keyTuple, SerializationTuple<V> valueTuple,
				SerializationTuple<?> hashKeyTuple, SerializationTuple<?> hashValueTuple,
				SerializationTuple<String> stringTuple) {

			this.keyTuple = keyTuple;
			this.valueTuple = valueTuple;
			this.hashKeyTuple = hashKeyTuple;
			this.hashValueTuple = hashValueTuple;
			this.stringTuple = stringTuple;
		}

		@Override
		public SerializationTuple<K> key() {
			return keyTuple;
		}

		@Override
		public SerializationTuple<V> value() {
			return valueTuple;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <HK> SerializationTuple<HK> hashKey() {
			return (SerializationTuple) hashKeyTuple;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <HV> SerializationTuple<HV> hashValue() {
			return (SerializationTuple) hashValueTuple;
		}

		@Override
		public SerializationTuple<String> string() {
			return stringTuple;
		}
	}
}
