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
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveZSetOperations}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultReactiveZSetOperations<K, V> implements ReactiveZSetOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final ReactiveSerializationContext<K, V> serializationContext;

	public DefaultReactiveZSetOperations(ReactiveRedisTemplate<?, ?> template,
			ReactiveSerializationContext<K, V> serializationContext) {

		Assert.notNull(template, "ReactiveRedisTemplate must not be null!");
		Assert.notNull(serializationContext, "ReactiveSerializationContext must not be null!");

		this.template = template;
		this.serializationContext = serializationContext;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#add(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Mono<Boolean> add(K key, V value, double score) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zAdd(rawKey(key), score, rawValue(value)).map(l -> l != 0));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#add(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<Long> addAll(K key, Collection<? extends TypedTuple<V>> tuples) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(tuples, "Key must not be null!");

		return createMono(connection -> {

			return Flux.fromIterable(tuples) //
					.map(t -> new DefaultTuple(ByteUtils.getBytes(rawValue(t.getValue())), t.getScore())) //
					.collectList() //
					.flatMap(serialized -> connection.zAdd(rawKey(key), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, Object... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		if (values.length == 1) {
			return createMono(connection -> connection.zRem(rawKey(key), rawValue((V) values[0])));
		}

		return createMono(connection -> {

			return Flux.fromArray((V[]) values) //
					.map(this::rawValue) //
					.collectList() //
					.flatMap(serialized -> connection.zRem(rawKey(key), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#incrementScore(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Mono<Double> incrementScore(K key, V value, double delta) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zIncrBy(rawKey(key), delta, rawValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rank(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> rank(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRank(rawKey(key), rawValue((V) o)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRank(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> reverseRank(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRevRank(rawKey(key), rawValue((V) o)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#range(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> range(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRange(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeWithScores(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> rangeWithScores(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRangeWithScores(rawKey(key), range).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByScore(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> rangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRangeByScore(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByScoreWithScores(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> rangeByScoreWithScores(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(
				connection -> connection.zRangeByScoreWithScores(rawKey(key), range).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByScore(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<V>> rangeByScore(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRangeByScore(rawKey(key), range, limit).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByScoreWithScores(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> rangeByScoreWithScores(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createMono(
				connection -> connection.zRangeByScoreWithScores(rawKey(key), range, limit).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> reverseRange(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRevRange(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeWithScores(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> reverseRangeWithScores(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRevRangeWithScores(rawKey(key), range).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByScore(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> reverseRangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRevRangeByScore(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> reverseRangeByScoreWithScores(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(
				connection -> connection.zRevRangeByScoreWithScores(rawKey(key), range).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByScore(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<V>> reverseRangeByScore(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRevRangeByScore(rawKey(key), range, limit).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<TypedTuple<V>>> reverseRangeByScoreWithScores(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createMono(
				connection -> connection.zRevRangeByScoreWithScores(rawKey(key), range, limit).map(this::readTypedTupleSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#count(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Long> count(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zCount(rawKey(key), range));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zCard(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#score(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Double> score(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zScore(rawKey(key), rawValue((V) o)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#removeRange(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Long> removeRange(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRemRangeByRank(rawKey(key), range));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#removeRangeByScore(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Long> removeRangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRemRangeByScore(rawKey(key), range));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#unionAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> unionAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		List<K> keys = getKeys(key, otherKeys);

		return createMono(connection -> {

			return Flux.fromIterable(keys) //
					.map(this::rawKey) //
					.collectList() //
					.flatMap(serialized -> connection.zUnionStore(rawKey(destKey), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#intersectAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Long> intersectAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		List<K> keys = getKeys(key, otherKeys);

		return createMono(connection -> {

			return Flux.fromIterable(keys) //
					.map(this::rawKey) //
					.collectList() //
					.flatMap(serialized -> connection.zInterStore(rawKey(destKey), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> rangeByLex(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRangeByLex(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<V>> rangeByLex(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createMono(connection -> connection.zRangeByLex(rawKey(key), range, limit).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByLex(java.lang.Object, org.springframework.data.domain.Range)
	 */
	@Override
	public Mono<Set<V>> reverseRangeByLex(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRevRangeByLex(rawKey(key), range).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#reverseRangeByLex(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Mono<Set<V>> reverseRangeByLex(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createMono(connection -> connection.zRevRangeByLex(rawKey(key), range, limit).map(this::readValueSet));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveZSetOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveZSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.zSetCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.key().write(key);
	}

	private List<K> getKeys(K key, Collection<K> otherKeys) {

		List<K> keys = new ArrayList<>(1 + otherKeys.size());

		keys.add(key);
		keys.addAll(otherKeys);

		return keys;
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.value().write(value);
	}

	private V readValue(ByteBuffer buffer) {
		return serializationContext.value().read(buffer);
	}

	private Set<V> readValueSet(Collection<ByteBuffer> raw) {

		Set<V> result = new LinkedHashSet<>(raw.size());

		for (ByteBuffer buffer : raw) {
			result.add(readValue(buffer));
		}

		return result;
	}

	private Set<TypedTuple<V>> readTypedTupleSet(Collection<Tuple> raw) {

		Set<TypedTuple<V>> result = new LinkedHashSet<>(raw.size());

		for (Tuple tuple : raw) {
			result.add(new DefaultTypedTuple<>(readValue(ByteBuffer.wrap(tuple.getValue())), tuple.getScore()));
		}

		return result;
	}
}
