/*
 * Copyright 2017-2022 the original author or authors.
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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveZSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Andrey Shlykov
 * @since 2.0
 */
class DefaultReactiveZSetOperations<K, V> implements ReactiveZSetOperations<K, V> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, V> serializationContext;

	public DefaultReactiveZSetOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<K, V> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	@Override
	public Mono<Boolean> add(K key, V value, double score) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zAdd(rawKey(key), score, rawValue(value)).map(l -> l != 0));
	}

	@Override
	public Mono<Long> addAll(K key, Collection<? extends TypedTuple<V>> tuples) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(tuples, "Key must not be null!");

		return createMono(connection -> Flux.fromIterable(tuples) //
				.map(t -> new DefaultTuple(ByteUtils.getBytes(rawValue(t.getValue())), t.getScore())) //
				.collectList() //
				.flatMap(serialized -> connection.zAdd(rawKey(key), serialized)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(K key, Object... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		if (values.length == 1) {
			return createMono(connection -> connection.zRem(rawKey(key), rawValue((V) values[0])));
		}

		return createMono(connection -> Flux.fromArray((V[]) values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serialized -> connection.zRem(rawKey(key), serialized)));
	}

	@Override
	public Mono<Double> incrementScore(K key, V value, double delta) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zIncrBy(rawKey(key), delta, rawValue(value)));
	}

	@Override
	public Mono<V> randomMember(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRandMember(rawKey(key))).map(this::readValue);
	}

	@Override
	public Flux<V> distinctRandomMembers(K key, long count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.isTrue(count > 0, "Negative count not supported. Use randomMembers to allow duplicate elements.");

		return createFlux(connection -> connection.zRandMember(rawKey(key), count)).map(this::readValue);
	}

	@Override
	public Flux<V> randomMembers(K key, long count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.isTrue(count > 0, "Use a positive number for count. This method is already allowing duplicate elements.");

		return createFlux(connection -> connection.zRandMember(rawKey(key), -count)).map(this::readValue);
	}

	@Override
	public Mono<TypedTuple<V>> randomMemberWithScore(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRandMemberWithScore(rawKey(key))).map(this::readTypedTuple);
	}

	@Override
	public Flux<TypedTuple<V>> distinctRandomMembersWithScore(K key, long count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.isTrue(count > 0, "Negative count not supported. Use randomMembers to allow duplicate elements.");

		return createFlux(connection -> connection.zRandMemberWithScore(rawKey(key), count)).map(this::readTypedTuple);
	}

	@Override
	public Flux<TypedTuple<V>> randomMembersWithScore(K key, long count) {

		Assert.notNull(key, "Key must not be null!");
		Assert.isTrue(count > 0, "Use a positive number for count. This method is already allowing duplicate elements.");

		return createFlux(connection -> connection.zRandMemberWithScore(rawKey(key), -count)).map(this::readTypedTuple);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> rank(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRank(rawKey(key), rawValue((V) o)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> reverseRank(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zRevRank(rawKey(key), rawValue((V) o)));
	}

	@Override
	public Flux<V> range(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRange(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> rangeWithScores(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRangeWithScores(rawKey(key), range).map(this::readTypedTuple));
	}

	@Override
	public Flux<V> rangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRangeByScore(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> rangeByScoreWithScores(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRangeByScoreWithScores(rawKey(key), range).map(this::readTypedTuple));
	}

	@Override
	public Flux<V> rangeByScore(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRangeByScore(rawKey(key), range, limit).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> rangeByScoreWithScores(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(
				connection -> connection.zRangeByScoreWithScores(rawKey(key), range, limit).map(this::readTypedTuple));
	}

	@Override
	public Flux<V> reverseRange(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRevRange(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> reverseRangeWithScores(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRevRangeWithScores(rawKey(key), range).map(this::readTypedTuple));
	}

	@Override
	public Flux<V> reverseRangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRevRangeByScore(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> reverseRangeByScoreWithScores(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(
				connection -> connection.zRevRangeByScoreWithScores(rawKey(key), range).map(this::readTypedTuple));
	}

	@Override
	public Flux<V> reverseRangeByScore(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRevRangeByScore(rawKey(key), range, limit).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> reverseRangeByScoreWithScores(K key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(
				connection -> connection.zRevRangeByScoreWithScores(rawKey(key), range, limit).map(this::readTypedTuple));
	}

	@Override
	public Flux<TypedTuple<V>> scan(K key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(options, "ScanOptions must not be null!");

		return createFlux(connection -> connection.zScan(rawKey(key), options).map(this::readTypedTuple));
	}

	@Override
	public Mono<Long> count(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zCount(rawKey(key), range));
	}

	@Override
	public Mono<Long> lexCount(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zLexCount(rawKey(key), range));
	}

	@Override
	public Mono<TypedTuple<V>> popMin(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zPopMin(rawKey(key)).map(this::readTypedTuple));
	}

	@Override
	public Flux<TypedTuple<V>> popMin(K key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.zPopMin(rawKey(key), count).map(this::readTypedTuple));
	}

	@Override
	public Mono<TypedTuple<V>> popMin(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		return createMono(connection -> connection.bZPopMin(rawKey(key), timeout).map(this::readTypedTuple));
	}

	@Override
	public Mono<TypedTuple<V>> popMax(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zPopMax(rawKey(key)).map(this::readTypedTuple));
	}

	@Override
	public Flux<TypedTuple<V>> popMax(K key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.zPopMax(rawKey(key), count).map(this::readTypedTuple));
	}

	@Override
	public Mono<TypedTuple<V>> popMax(K key, Duration timeout) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(timeout, "Timeout must not be null!");

		return createMono(connection -> connection.bZPopMax(rawKey(key), timeout).map(this::readTypedTuple));
	}

	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zCard(rawKey(key)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Double> score(K key, Object o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.zScore(rawKey(key), rawValue((V) o)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<List<Double>> score(K key, Object... o) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> Flux.fromArray((V[]) o) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(values -> connection.zMScore(rawKey(key), values)));
	}

	@Override
	public Mono<Long> removeRange(K key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRemRangeByRank(rawKey(key), range));
	}

	@Override
	public Mono<Long> removeRangeByLex(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRemRangeByLex(rawKey(key), range));
	}

	@Override
	public Mono<Long> removeRangeByScore(K key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createMono(connection -> connection.zRemRangeByScore(rawKey(key), range));
	}

	@Override
	public Flux<V> difference(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zDiff).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> differenceWithScores(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zDiffWithScores).map(this::readTypedTuple));
	}

	@Override
	public Mono<Long> differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.zDiffStore(rawKey(destKey), serialized)));

	}

	@Override
	public Flux<V> intersect(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zInter).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> intersectWithScores(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zInterWithScores).map(this::readTypedTuple));
	}

	@Override
	public Flux<TypedTuple<V>> intersectWithScores(K key, Collection<K> otherKeys, Aggregate aggregate, Weights weights) {

		// TODO: Inconsistent method signatures Aggregate/Weights vs Weights/Aggregate in Connection API

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(aggregate, "Aggregate must not be null!");
		Assert.notNull(weights, "Weights must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(sets -> connection.zInterWithScores(sets, weights, aggregate)).map(this::readTypedTuple));
	}

	@Override
	public Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.zInterStore(rawKey(destKey), serialized)));
	}

	@Override
	public Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(aggregate, "Aggregate must not be null!");
		Assert.notNull(weights, "Weights must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.zInterStore(rawKey(destKey), serialized, weights, aggregate)));
	}

	@Override
	public Flux<V> union(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zUnion).map(this::readValue));
	}

	@Override
	public Flux<TypedTuple<V>> unionWithScores(K key, Collection<K> otherKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(connection::zUnionWithScores).map(this::readTypedTuple));
	}

	@Override
	public Flux<TypedTuple<V>> unionWithScores(K key, Collection<K> otherKeys, Aggregate aggregate, Weights weights) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(aggregate, "Aggregate must not be null!");
		Assert.notNull(weights, "Weights must not be null!");

		return createFlux(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMapMany(sets -> connection.zUnionWithScores(sets, weights, aggregate)).map(this::readTypedTuple));
	}

	@Override
	public Mono<Long> unionAndStore(K key, K otherKey, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKey, "Other key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.zUnionStore(rawKey(destKey), serialized)));
	}

	@Override
	public Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(otherKeys, "Other keys must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(aggregate, "Aggregate must not be null!");
		Assert.notNull(weights, "Weights must not be null!");

		return createMono(connection -> Flux.fromIterable(getKeys(key, otherKeys)) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.zUnionStore(rawKey(destKey), serialized, weights, aggregate)));
	}

	@Override
	public Flux<V> rangeByLex(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRangeByLex(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<V> rangeByLex(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.zRangeByLex(rawKey(key), range, limit).map(this::readValue));
	}

	@Override
	public Flux<V> reverseRangeByLex(K key, Range<String> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		return createFlux(connection -> connection.zRevRangeByLex(rawKey(key), range).map(this::readValue));
	}

	@Override
	public Flux<V> reverseRangeByLex(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.zRevRangeByLex(rawKey(key), range, limit).map(this::readValue));
	}

	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.doCreateMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveZSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.doCreateMono(connection -> function.apply(connection.zSetCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveZSetCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.doCreateFlux(connection -> function.apply(connection.zSetCommands()));
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

	private TypedTuple<V> readTypedTuple(Tuple raw) {
		return new DefaultTypedTuple<>(readValue(ByteBuffer.wrap(raw.getValue())), raw.getScore());
	}
}
