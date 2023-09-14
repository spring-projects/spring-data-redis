/*
 * Copyright 2017-2023 the original author or authors.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.ReactiveListCommands.Direction;
import org.springframework.data.redis.connection.ReactiveListCommands.LPosCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 * @since 2.0
 */
class DefaultReactiveListOperations<K, V> implements ReactiveListOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, V> serializationContext;

	DefaultReactiveListOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<K, V> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	@Override
	public Flux<V> range(K key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(connection -> connection.lRange(rawKey(key), start, end).map(this::readRequiredValue));
	}

	@Override
	public Mono<Boolean> trim(K key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lTrim(rawKey(key), start, end));
	}

	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lLen(rawKey(key)));
	}

	@Override
	public Mono<Long> leftPush(K key, V value) {
		return leftPushAll(key, value);
	}

	@Override
	@SafeVarargs
	public final Mono<Long> leftPushAll(K key, V... values) {

		Assert.notEmpty(values, "Values must not be null or empty");

		return leftPushAll(key, Arrays.asList(values));
	}

	@Override
	public Mono<Long> leftPushAll(K key, Collection<V> values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(values, "Values must not be null or empty");

		return createMono(listCommands -> Flux.fromIterable(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> listCommands.lPush(rawKey(key), serialized)));
	}

	@Override
	public Mono<Long> leftPushIfPresent(K key, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lPushX(rawKey(key), rawValue(value)));
	}

	@Override
	public Mono<Long> leftPush(K key, V pivot, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands ->
				listCommands.lInsert(rawKey(key), Position.BEFORE, rawValue(pivot), rawValue(value)));
	}

	@Override
	public Mono<Long> rightPush(K key, V value) {
		return rightPushAll(key, value);
	}

	@Override
	@SafeVarargs
	public final Mono<Long> rightPushAll(K key, V... values) {

		Assert.notNull(values, "Values must not be null");

		return rightPushAll(key, Arrays.asList(values));
	}

	@Override
	public Mono<Long> rightPushAll(K key, Collection<V> values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notEmpty(values, "Values must not be null or empty");

		return createMono(listCommands -> Flux.fromIterable(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> listCommands.rPush(rawKey(key), serialized)));
	}

	@Override
	public Mono<Long> rightPushIfPresent(K key, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.rPushX(rawKey(key), rawValue(value)));
	}

	@Override
	public Mono<Long> rightPush(K key, V pivot, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands ->
				listCommands.lInsert(rawKey(key), Position.AFTER, rawValue(pivot), rawValue(value)));
	}

	@Override
	public Mono<V> move(K sourceKey, Direction from, K destinationKey, Direction to) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		return createMono(connection -> connection.lMove(rawKey(sourceKey), rawKey(destinationKey), from, to)
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<V> move(K sourceKey, Direction from, K destinationKey, Direction to, Duration timeout) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");
		Assert.notNull(timeout, "Timeout must not be null");

		return createMono(connection -> connection.bLMove(rawKey(sourceKey), rawKey(destinationKey), from, to, timeout)
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<Boolean> set(K key, long index, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lSet(rawKey(key), index, rawValue(value)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, long count, Object value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lRem(rawKey(key), count, rawValue((V) value)));
	}

	@Override
	public Mono<V> index(K key, long index) {

		Assert.notNull(key, "Key must not be null");

		return createMono(connection -> connection.lIndex(rawKey(key), index).map(this::readRequiredValue));
	}

	@Override
	public Mono<Long> indexOf(K key, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.lPos(rawKey(key), rawValue(value)));
	}

	@Override
	public Mono<Long> lastIndexOf(K key, V value) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands ->
				listCommands.lPos(LPosCommand.lPosOf(rawValue(value)).from(rawKey(key)).rank(-1)));
	}

	@Override
	public Mono<V> leftPop(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(connection -> connection.lPop(rawKey(key)).map(this::readRequiredValue));

	}

	@Override
	public Flux<V> leftPop(K key, long count) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(listCommands -> listCommands.lPop(rawKey(key), count).map(this::readRequiredValue));
	}

	@Override
	public Mono<V> leftPop(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(timeout, "Duration must not be null");
		Assert.isTrue(isZeroOrGreaterOneSecond(timeout), "Duration must be either zero or greater or equal to 1 second");

		return createMono(connection -> connection.blPop(Collections.singletonList(rawKey(key)), timeout)
				.mapNotNull(popResult -> readValue(popResult.getValue())));
	}

	@Override
	public Mono<V> rightPop(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(listCommands -> listCommands.rPop(rawKey(key)).map(this::readRequiredValue));
	}

	@Override
	public Flux<V> rightPop(K key, long count) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(listCommands -> listCommands.rPop(rawKey(key), count).map(this::readRequiredValue));
	}

	@Override
	public Mono<V> rightPop(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(timeout, "Duration must not be null");
		Assert.isTrue(isZeroOrGreaterOneSecond(timeout), "Duration must be either zero or greater or equal to 1 second");

		return createMono(connection -> connection.brPop(Collections.singletonList(rawKey(key)), timeout)
				.mapNotNull(popResult -> readValue(popResult.getValue())));
	}

	@Override
	public Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");

		return createMono(connection -> connection.rPopLPush(rawKey(sourceKey), rawKey(destinationKey))
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey, Duration timeout) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(timeout, "Duration must not be null");
		Assert.isTrue(isZeroOrGreaterOneSecond(timeout), "Duration must be either zero or greater or equal to 1 second");

		return createMono(connection -> connection.bRPopLPush(rawKey(sourceKey), rawKey(destinationKey), timeout)
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveListCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateMono(connection -> function.apply(connection.listCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveListCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateFlux(connection -> function.apply(connection.listCommands()));
	}

	private boolean isZeroOrGreaterOneSecond(Duration timeout) {
		return timeout.isZero() || timeout.getNano() % TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS) == 0;
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.getValueSerializationPair().write(value);
	}

	@Nullable
	private V readValue(ByteBuffer buffer) {
		return serializationContext.getValueSerializationPair().read(buffer);
	}

	private V readRequiredValue(ByteBuffer buffer) {

		V value = readValue(buffer);

		if (value != null) {
			return value;
		}

		throw new InvalidDataAccessApiUsageException("Deserialized list value is null");
	}
}
