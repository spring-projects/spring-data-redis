/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor
class DefaultReactiveListOperations<K, V> implements ReactiveListOperations<K, V> {

	private final @NonNull ReactiveRedisTemplate<?, ?> template;
	private final @NonNull RedisSerializationContext<K, V> serializationContext;

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#range(java.lang.Object, long, long)
	 */
	@Override
	public Flux<V> range(K key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.lRange(rawKey(key), start, end).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#trim(java.lang.Object, long, long)
	 */
	@Override
	public Mono<Boolean> trim(K key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lTrim(rawKey(key), start, end));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lLen(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> leftPush(K key, V value) {
		return leftPushAll(key, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPushAll(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> leftPushAll(K key, V... values) {

		Assert.notEmpty(values, "Values must not be null or empty!");

		return leftPushAll(key, Arrays.asList(values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPushAll(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Long> leftPushAll(K key, Collection<V> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notEmpty(values, "Values must not be null or empty!");

		return createMono(connection -> Flux.fromIterable(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> connection.lPush(rawKey(key), serialized)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPushIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> leftPushIfPresent(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lPushX(rawKey(key), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPush(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> leftPush(K key, V pivot, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lInsert(rawKey(key), Position.BEFORE, rawValue(pivot), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> rightPush(K key, V value) {
		return rightPushAll(key, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPushAll(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> rightPushAll(K key, V... values) {

		Assert.notNull(values, "Values must not be null!");

		return rightPushAll(key, Arrays.asList(values));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPushAll(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Long> rightPushAll(K key, Collection<V> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notEmpty(values, "Values must not be null or empty!");

		return createMono(connection -> Flux.fromIterable(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> connection.rPush(rawKey(key), serialized)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPushIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> rightPushIfPresent(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.rPushX(rawKey(key), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPush(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> rightPush(K key, V pivot, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lInsert(rawKey(key), Position.AFTER, rawValue(pivot), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#set(java.lang.Object, long, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> set(K key, long index, V value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lSet(rawKey(key), index, rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#remove(java.lang.Object, long, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, long count, Object value) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lRem(rawKey(key), count, rawValue((V) value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#index(java.lang.Object, long)
	 */
	@Override
	public Mono<V> index(K key, long index) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lIndex(rawKey(key), index).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPop(java.lang.Object)
	 */
	@Override
	public Mono<V> leftPop(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.lPop(rawKey(key)).map(this::readValue));

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#leftPop(java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<V> leftPop(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");
		Assert.isTrue(isZeroOrGreater1Second(timeout), "Duration must be either zero or greater or equal to 1 second!");

		return createMono(connection -> connection.blPop(Collections.singletonList(rawKey(key)), timeout)
				.map(popResult -> readValue(popResult.getValue())));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPop(java.lang.Object)
	 */
	@Override
	public Mono<V> rightPop(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.rPop(rawKey(key)).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPop(java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<V> rightPop(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");
		Assert.isTrue(isZeroOrGreater1Second(timeout), "Duration must be either zero or greater or equal to 1 second!");

		return createMono(connection -> connection.brPop(Collections.singletonList(rawKey(key)), timeout)
				.map(popResult -> readValue(popResult.getValue())));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPopAndLeftPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(destinationKey, "Destination key must not be null!");

		return createMono(
				connection -> connection.rPopLPush(rawKey(sourceKey), rawKey(destinationKey)).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#rightPopAndLeftPush(java.lang.Object, java.lang.Object, java.time.Duration)
	 */
	@Override
	public Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey, Duration timeout) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(destinationKey, "Destination key must not be null!");
		Assert.notNull(timeout, "Duration must not be null!");
		Assert.isTrue(isZeroOrGreater1Second(timeout), "Duration must be either zero or greater or equal to 1 second!");

		return createMono(
				connection -> connection.bRPopLPush(rawKey(sourceKey), rawKey(destinationKey), timeout).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveListOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveListCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.listCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveListCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createFlux(connection -> function.apply(connection.listCommands()));
	}

	private boolean isZeroOrGreater1Second(Duration timeout) {
		return timeout.isZero() || timeout.getNano() % TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS) == 0;
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
}
