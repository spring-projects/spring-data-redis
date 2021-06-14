/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveValueOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jiahe Cai
 * @since 2.0
 */
class DefaultReactiveValueOperations<K, V> implements ReactiveValueOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, V> serializationContext;

	DefaultReactiveValueOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<K, V> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#set(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> set(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.set(rawKey(key), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#set(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Mono<Boolean> set(K key, V value, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");

		return createMono(
				connection -> connection.set(rawKey(key), rawValue(value), Expiration.from(timeout), SetOption.UPSERT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#setIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> setIfAbsent(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(
				connection -> connection.set(rawKey(key), rawValue(value), Expiration.persistent(), SetOption.SET_IF_ABSENT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#setIfAbsent(java.lang.Object, java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<Boolean> setIfAbsent(K key, V value, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");

		return createMono(
				connection -> connection.set(rawKey(key), rawValue(value), Expiration.from(timeout), SetOption.SET_IF_ABSENT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#setIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> setIfPresent(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(
				connection -> connection.set(rawKey(key), rawValue(value), Expiration.persistent(), SetOption.SET_IF_PRESENT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#setIfPresent(java.lang.Object, java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<Boolean> setIfPresent(K key, V value, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");

		return createMono(
				connection -> connection.set(rawKey(key), rawValue(value), Expiration.from(timeout), SetOption.SET_IF_PRESENT));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#multiSet(java.util.Map)
	 */
	@Override
	public Mono<Boolean> multiSet(Map<? extends K, ? extends V> map) {

		Assert.notNull(map, "Map must not be null!");

		return createMono(connection -> {

			Mono<Map<ByteBuffer, ByteBuffer>> serializedMap = Flux.fromIterable(() -> map.entrySet().iterator())
					.collectMap(entry -> rawKey(entry.getKey()), entry -> rawValue(entry.getValue()));

			return serializedMap.flatMap(connection::mSet);
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#multiSetIfAbsent(java.util.Map)
	 */
	@Override
	public Mono<Boolean> multiSetIfAbsent(Map<? extends K, ? extends V> map) {

		Assert.notNull(map, "Map must not be null!");

		return createMono(connection -> {

			Mono<Map<ByteBuffer, ByteBuffer>> serializedMap = Flux.fromIterable(() -> map.entrySet().iterator())
					.collectMap(entry -> rawKey(entry.getKey()), entry -> rawValue(entry.getValue()));

			return serializedMap.flatMap(connection::mSetNX);
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#get(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Mono<V> get(Object key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.get(rawKey((K) key)) //
				.map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#getAndDelete(java.lang.Object)
	 */
	@Override
	public Mono<V> getAndDelete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.getDel(rawKey(key)) //
				.map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#getAndExpire(java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<V> getAndExpire(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		return createMono(connection -> connection.getEx(rawKey(key), Expiration.from(timeout)) //
				.map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#getAndPersist(java.lang.Object)
	 */
	@Override
	public Mono<V> getAndPersist(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.getEx(rawKey(key), Expiration.persistent()) //
				.map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#getAndSet(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<V> getAndSet(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.getSet(rawKey(key), rawValue(value)).map(value()::read));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#multiGet(java.util.Collection)
	 */
	@Override
	public Mono<List<V>> multiGet(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return createMono(connection -> Flux.fromIterable(keys).map(key()::write).collectList().flatMap(connection::mGet)
				.map(this::deserializeValues));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#increment(java.lang.Object)
	 */
	@Override
	public Mono<Long> increment(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.numberCommands().incr(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#increment(java.lang.Object, long)
	 */
	@Override
	public Mono<Long> increment(K key, long delta) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.numberCommands().incrBy(rawKey(key), delta));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#increment(java.lang.Object, double)
	 */
	@Override
	public Mono<Double> increment(K key, double delta) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.numberCommands().incrBy(rawKey(key), delta));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#decrement(java.lang.Object)
	 */
	@Override
	public Mono<Long> decrement(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.numberCommands().decr(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#decrement(java.lang.Object, long)
	 */
	@Override
	public Mono<Long> decrement(K key, long delta) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.numberCommands().decrBy(rawKey(key), delta));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#append(java.lang.Object, java.lang.String)
	 */
	@Override
	public Mono<Long> append(K key, String value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return createMono(
				connection -> connection.append(rawKey(key), serializationContext.getStringSerializationPair().write(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#get(java.lang.Object, long, long)
	 */
	@Override
	public Mono<String> get(K key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.getRange(rawKey(key), start, end) //
				.map(stringSerializationPair()::read));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#set(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public Mono<Long> set(K key, V value, long offset) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.setRange(rawKey(key), rawValue(value), offset));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.strLen(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#setBit(java.lang.Object, long, boolean)
	 */
	@Override
	public Mono<Boolean> setBit(K key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.setBit(rawKey(key), offset, value));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#getBit(java.lang.Object, long)
	 */
	@Override
	public Mono<Boolean> getBit(K key, long offset) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.getBit(rawKey(key), offset));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#bitField(java.lang.Object, org.springframework.data.redis.connection.BitFieldSubCommands)
	 */
	@Override
	public Mono<List<Long>> bitField(K key, BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(subCommands, "BitFieldSubCommands must not be null!");

		return createMono(connection -> connection.bitField(rawKey(key), subCommands));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveValueOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveStringCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.stringCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.getValueSerializationPair().write(value);
	}

	private V readValue(ByteBuffer buffer) {
		return serializationContext.getValueSerializationPair().read(buffer);
	}

	private SerializationPair<String> stringSerializationPair() {
		return serializationContext.getStringSerializationPair();
	}

	private SerializationPair<K> key() {
		return serializationContext.getKeySerializationPair();
	}

	private SerializationPair<V> value() {
		return serializationContext.getValueSerializationPair();
	}

	private List<V> deserializeValues(List<ByteBuffer> source) {

		List<V> result = new ArrayList<>(source.size());

		for (ByteBuffer buffer : source) {

			if (buffer == null) {
				result.add(null);
			} else {
				result.add(readValue(buffer));
			}
		}

		return result;
	}
}
