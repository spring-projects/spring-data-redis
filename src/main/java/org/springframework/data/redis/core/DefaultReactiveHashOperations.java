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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class DefaultReactiveHashOperations<H, HK, HV> implements ReactiveHashOperations<H, HK, HV> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<H, ?> serializationContext;

	DefaultReactiveHashOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<H, ?> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#delete(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(H key, Object... hashKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKeys, "Hash keys must not be null!");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty!");
		Assert.noNullElements(hashKeys, "Hash keys must not contain null elements!");

		return createMono(connection -> Flux.fromArray(hashKeys) //
				.map(o -> (HK) o).map(this::rawHashKey) //
				.collectList() //
				.flatMap(hks -> connection.hDel(rawKey(key), hks)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#hasKey(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Boolean> hasKey(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return createMono(connection -> connection.hExists(rawKey(key), rawHashKey((HK) hashKey)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#get(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<HV> get(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return createMono(connection -> connection.hGet(rawKey(key), rawHashKey((HK) hashKey)).map(this::readHashValue));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#multiGet(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<List<HV>> multiGet(H key, Collection<HK> hashKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKeys, "Hash keys must not be null!");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty!");

		return createMono(connection -> Flux.fromIterable(hashKeys) //
				.map(this::rawHashKey) //
				.collectList() //
				.flatMap(hks -> connection.hMGet(rawKey(key), hks)).map(this::deserializeHashValues));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#increment(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public Mono<Long> increment(H key, HK hashKey, long delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return template.createMono(connection -> connection //
				.numberCommands() //
				.hIncrBy(rawKey(key), rawHashKey(hashKey), delta));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#increment(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Mono<Double> increment(H key, HK hashKey, double delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return template.createMono(connection -> connection //
				.numberCommands() //
				.hIncrBy(rawKey(key), rawHashKey(hashKey), delta));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#randomField(H)
	 */
	@Override
	public Mono<HK> randomField(H key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection //
				.hashCommands().hRandField(rawKey(key))).map(this::readHashKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#randomValue(H)
	 */
	@Override
	public Mono<Map.Entry<HK, HV>> randomValue(H key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection //
				.hashCommands().hRandFieldWithValues(rawKey(key))).map(this::deserializeHashEntry);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#randomFields(H, long)
	 */
	@Override
	public Flux<HK> randomFields(H key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return template.createFlux(connection -> connection //
				.hashCommands().hRandField(rawKey(key), count)).map(this::readHashKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#randomValues(H, long)
	 */
	@Override
	public Flux<Map.Entry<HK, HV>> randomValues(H key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return template.createFlux(connection -> connection //
				.hashCommands().hRandFieldWithValues(rawKey(key), count)).map(this::deserializeHashEntry);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#keys(java.lang.Object)
	 */
	@Override
	public Flux<HK> keys(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.hKeys(rawKey(key)) //
				.map(this::readHashKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.hLen(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#putAll(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<Boolean> putAll(H key, Map<? extends HK, ? extends HV> map) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(map, "Map must not be null!");

		return createMono(connection -> Flux.fromIterable(() -> map.entrySet().iterator()) //
				.collectMap(entry -> rawHashKey(entry.getKey()), entry -> rawHashValue(entry.getValue())) //
				.flatMap(serialized -> connection.hMSet(rawKey(key), serialized)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#put(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> put(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");
		Assert.notNull(value, "Hash value must not be null!");

		return createMono(connection -> connection.hSet(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#putIfAbsent(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> putIfAbsent(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");
		Assert.notNull(value, "Hash value must not be null!");

		return createMono(connection -> connection.hSetNX(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#values(java.lang.Object)
	 */
	@Override
	public Flux<HV> values(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.hVals(rawKey(key)) //
				.map(this::readHashValue));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#entries(java.lang.Object)
	 */
	@Override
	public Flux<Map.Entry<HK, HV>> entries(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.hGetAll(rawKey(key)) //
				.map(this::deserializeHashEntry));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#scan(java.lang.Object, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Flux<Map.Entry<HK, HV>> scan(H key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(key, "ScanOptions must not be null!");

		return createFlux(connection -> connection.hScan(rawKey(key), options) //
				.map(this::deserializeHashEntry));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(H key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveHashCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.hashCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveHashCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createFlux(connection -> function.apply(connection.hashCommands()));
	}

	private ByteBuffer rawKey(H key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashKey(HK key) {
		return serializationContext.getHashKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashValue(HV key) {
		return serializationContext.getHashValueSerializationPair().write(key);
	}

	@SuppressWarnings("unchecked")
	private HK readHashKey(ByteBuffer value) {
		return (HK) serializationContext.getHashKeySerializationPair().read(value);
	}

	@SuppressWarnings("unchecked")
	private HV readHashValue(ByteBuffer value) {
		return (HV) (value == null ? value : serializationContext.getHashValueSerializationPair().read(value));
	}

	private Map.Entry<HK, HV> deserializeHashEntry(Map.Entry<ByteBuffer, ByteBuffer> source) {
		return Collections.singletonMap(readHashKey(source.getKey()), readHashValue(source.getValue())).entrySet()
				.iterator().next();
	}

	private List<HV> deserializeHashValues(List<ByteBuffer> source) {

		List<HV> values = new ArrayList<>(source.size());
		for (ByteBuffer byteBuffer : source) {
			values.add(readHashValue(byteBuffer));
		}
		return values;
	}
}
