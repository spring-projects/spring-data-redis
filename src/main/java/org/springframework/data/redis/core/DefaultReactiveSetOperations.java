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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveSetCommands;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Roman Bezpalko
 * @author John Blum
 * @author Mingi Lee
 * @since 2.0
 */
class DefaultReactiveSetOperations<K, V> implements ReactiveSetOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, V> serializationContext;

	DefaultReactiveSetOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<K, V> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> add(K key, V... values) {

		Assert.notNull(key, "Key must not be null");

		if (values.length == 1) {
			return createMono(setCommands -> setCommands.sAdd(rawKey(key), rawValue(values[0])));
		}

		return createMono(setCommands -> Flux.fromArray(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> setCommands.sAdd(rawKey(key), serialized)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, Object... values) {

		Assert.notNull(key, "Key must not be null");

		if (values.length == 1) {
			return createMono(setCommands -> setCommands.sRem(rawKey(key), rawValue((V) values[0])));
		}

		return createMono(setCommands -> Flux.fromArray((V[]) values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> setCommands.sRem(rawKey(key), serialized)));
	}

	@Override
	public Mono<V> pop(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(setCommands -> setCommands.sPop(rawKey(key)).map(this::readRequiredValue));
	}

	@Override
	public Flux<V> pop(K key, long count) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(setCommands -> setCommands.sPop(rawKey(key), count).map(this::readRequiredValue));
	}

	@Override
	public Mono<Boolean> move(K sourceKey, V value, K destKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return createMono(setCommands -> setCommands.sMove(rawKey(sourceKey), rawKey(destKey), rawValue(value)));
	}

	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(setCommands -> setCommands.sCard(rawKey(key)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Boolean> isMember(K key, Object o) {

		Assert.notNull(key, "Key must not be null");

		return createMono(setCommands -> setCommands.sIsMember(rawKey(key), rawValue((V) o)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Map<Object, Boolean>> isMember(K key, Object... objects) {

		Assert.notNull(key, "Key must not be null");

		return createMono(setCommands -> Flux.fromArray((V[]) objects) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(rawValues -> setCommands.sMIsMember(rawKey(key), rawValues)) //
				.map(result -> {

					Map<Object, Boolean> isMember = new LinkedHashMap<>(result.size());

					for (int i = 0; i < objects.length; i++) {
						isMember.put(objects[i], result.get(i));
					}

					return isMember;
				}));
	}

	@Override
	public Flux<V> intersect(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");

		return intersect(key, Collections.singleton(otherKey));
	}

	@Override
	public Flux<V> intersect(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");

		return intersect(getKeys(key, otherKeys));
	}

	@Override
	public Flux<V> intersect(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null");

		return createFlux(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(setCommands::sInter) //
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<Long> intersectAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return intersectAndStore(getKeys(key, otherKeys), destKey);
	}

	@Override
	public Mono<Long> intersectAndStore(Collection<K> keys, K destKey) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return createMono(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> setCommands.sInterStore(rawKey(destKey), rawKeys)));
	}

	@Override
	public Mono<Long> intersectSize(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");

		return intersectSize(key, Collections.singleton(otherKey));
	}

	@Override
	public Mono<Long> intersectSize(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");

		return intersectSize(getKeys(key, otherKeys));
	}

	@Override
	public Mono<Long> intersectSize(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null");

		return createMono(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(setCommands::sInterCard));
	}

	@Override
	public Flux<V> union(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");

		return union(key, Collections.singleton(otherKey));
	}

	@Override
	public Flux<V> union(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");

		return union(getKeys(key, otherKeys));
	}

	@Override
	public Flux<V> union(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null");

		return createFlux(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(setCommands::sUnion) //
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<Long> unionAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return unionAndStore(getKeys(key, otherKeys), destKey);
	}

	@Override
	public Mono<Long> unionAndStore(Collection<K> keys, K destKey) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return createMono(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> setCommands.sUnionStore(rawKey(destKey), rawKeys)));
	}

	@Override
	public Flux<V> difference(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");

		return difference(key, Collections.singleton(otherKey));
	}

	@Override
	public Flux<V> difference(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");

		return difference(getKeys(key, otherKeys));
	}

	@Override
	public Flux<V> difference(Collection<K> keys) {

		Assert.notNull(keys, "Keys must not be null");

		return createFlux(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(setCommands::sDiff) //
				.map(this::readRequiredValue));
	}

	@Override
	public Mono<Long> differenceAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKey, "Other key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return differenceAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Mono<Long> differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(otherKeys, "Other keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return differenceAndStore(getKeys(key, otherKeys), destKey);
	}

	@Override
	public Mono<Long> differenceAndStore(Collection<K> keys, K destKey) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.notNull(destKey, "Destination key must not be null");

		return createMono(setCommands -> Flux.fromIterable(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> setCommands.sDiffStore(rawKey(destKey), rawKeys)));
	}

	@Override
	public Flux<V> members(K key) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(setCommands -> setCommands.sMembers(rawKey(key)).map(this::readRequiredValue));
	}

	@Override
	public Flux<V> scan(K key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(options, "ScanOptions must not be null");

		return createFlux(setCommands -> setCommands.sScan(rawKey(key), options).map(this::readRequiredValue));
	}

	@Override
	public Mono<V> randomMember(K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(setCommands -> setCommands.sRandMember(rawKey(key)).map(this::readRequiredValue));
	}

	@Override
	public Flux<V> distinctRandomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Negative count not supported; Use randomMembers to allow duplicate elements");

		return createFlux(setCommands -> setCommands.sRandMember(rawKey(key), count).map(this::readRequiredValue));
	}

	@Override
	public Flux<V> randomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Use a positive number for count; This method is already allowing duplicate elements");

		return createFlux(setCommands -> setCommands.sRandMember(rawKey(key), -count).map(this::readRequiredValue));
	}

	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateMono(connection -> function.apply(connection.setCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateFlux(connection -> function.apply(connection.setCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private List<K> getKeys(K key, Collection<K> otherKeys) {

		List<K> keys = new ArrayList<>(1 + otherKeys.size());

		keys.add(key);
		keys.addAll(otherKeys);

		return keys;
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.getValueSerializationPair().write(value);
	}

	private @Nullable V readValue(ByteBuffer buffer) {
		return serializationContext.getValueSerializationPair().read(buffer);
	}

	private V readRequiredValue(ByteBuffer buffer) {

		V value = readValue(buffer);

		if (value != null) {
			return value;
		}

		throw new InvalidDataAccessApiUsageException("Deserialized set value is null");
	}
}
