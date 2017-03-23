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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveSetCommands;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class DefaultReactiveSetOperations<K, V> implements ReactiveSetOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final ReactiveSerializationContext<K, V> serializationContext;

	/**
	 * Creates new {@link DefaultReactiveSetOperations}.
	 *
	 * @param template must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 */
	public DefaultReactiveSetOperations(ReactiveRedisTemplate<?, ?> template,
			ReactiveSerializationContext<K, V> serializationContext) {

		Assert.notNull(template, "ReactiveRedisTemplate must not be null!");
		Assert.notNull(serializationContext, "ReactiveSerializationContext must not be null!");

		this.template = template;
		this.serializationContext = serializationContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#add(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Mono<Long> add(K key, V... values) {

		Assert.notNull(key, "Key must not be null!");

		if (values.length == 1) {
			return createMono(connection -> connection.sAdd(rawKey(key), rawValue(values[0])));
		}

		return createMono(connection -> Flux.fromArray(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> connection.sAdd(rawKey(key), serialized)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, Object... values) {

		Assert.notNull(key, "Key must not be null!");

		if (values.length == 1) {
			return createMono(connection -> connection.sRem(rawKey(key), rawValue((V) values[0])));
		}

		return createMono(connection -> Flux.fromArray((V[]) values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> connection.sRem(rawKey(key), serialized)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#pop(java.lang.Object)
	 */
	@Override
	public Mono<V> pop(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.sPop(rawKey(key)).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#move(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> move(K sourceKey, V value, K destKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> connection.sMove(rawKey(sourceKey), rawKey(destKey), rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.sCard(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#isMember(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Boolean> isMember(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.sIsMember(rawKey(key), rawValue((V) o)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#intersect(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Set<V>> intersect(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");

		return intersect(key, Collections.singleton(otherKey));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#intersect(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Set<V>> intersect(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(connection::sInter) //
				.map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#intersectAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> intersectAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> connection.sInterStore(rawKey(destKey), rawKeys)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#union(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Set<V>> union(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");

		return union(key, Collections.singleton(otherKey));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#union(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Set<V>> union(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(connection::sUnion) //
				.map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#unionAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> unionAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> connection.sUnionStore(rawKey(destKey), rawKeys)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#difference(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Set<V>> difference(K key, K otherKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");

		return difference(key, Collections.singleton(otherKey));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#difference(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Set<V>> difference(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(connection::sDiff) //
				.map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#differenceAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> differenceAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return differenceAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#differenceAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Mono<Long> differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(rawKeys -> connection.sDiffStore(rawKey(destKey), rawKeys)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#members(java.lang.Object)
	 */
	@Override
	public Mono<Set<V>> members(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.sMembers(rawKey(key)).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#randomMember(java.lang.Object)
	 */
	@Override
	public Mono<V> randomMember(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.sRandMember(rawKey(key)).map(this::readValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#distinctRandomMembers(java.lang.Object, long)
	 */
	@Override
	public Mono<Set<V>> distinctRandomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Negative count not supported. Use randomMembers to allow duplicate elements.");

		return createMono(connection -> connection.sRandMember(rawKey(key), count).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#randomMembers(java.lang.Object, long)
	 */
	@Override
	public Mono<List<V>> randomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Use a positive number for count. This method is already allowing duplicate elements.");

		return createMono(connection -> connection.sRandMember(rawKey(key), -count).map(this::readValueList));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveSetOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.setCommands()));
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

	private V readValue(ByteBuffer buffer) {
		return serializationContext.getValueSerializationPair().read(buffer);
	}

	private Set<V> readValueSet(Collection<ByteBuffer> raw) {

		Set<V> result = new LinkedHashSet<>(raw.size());

		for (ByteBuffer buffer : raw) {
			result.add(readValue(buffer));
		}

		return result;
	}

	private List<V> readValueList(Collection<ByteBuffer> raw) {

		List<V> result = new ArrayList<>(raw.size());

		for (ByteBuffer buffer : raw) {
			result.add(readValue(buffer));
		}

		return result;
	}
}
