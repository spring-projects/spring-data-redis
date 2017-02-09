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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultReactiveHashOperations<H, HK, HV> implements ReactiveHashOperations<H, HK, HV> {

	private final ReactiveRedisTemplate<H, ?> template;

	public DefaultReactiveHashOperations(ReactiveRedisTemplate<H, ?> template) {
		this.template = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#delete(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> delete(H key, Object... hashKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKeys, "Hash keys must not be null!");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty!");
		Assert.noNullElements(hashKeys, "Hash keys must not contain null elements!");

		return createMono(connection -> {

			return Flux.fromArray(hashKeys) //
					.map(o -> (HK) o).map(this::rawHashKey) //
					.collectList() //
					.then(hks -> connection.hDel(rawKey(key), hks));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#hasKey(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Boolean> hasKey(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return createMono(connection -> connection.hExists(rawKey(key), rawHashKey((HK) hashKey)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#get(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<HV> get(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");

		return createMono(connection -> connection.hGet(rawKey(key), rawHashKey((HK) hashKey)).map(this::readHashValue));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#multiGet(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Mono<List<HV>> multiGet(H key, Collection<HK> hashKeys) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKeys, "Hash keys must not be null!");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty!");

		return createMono(connection -> {

			return Flux.fromIterable(hashKeys) //
					.map(this::rawHashKey) //
					.collectList() //
					.then(hks -> connection.hMGet(rawKey(key), hks)).map(byteBuffers -> {

						List<HV> values = new ArrayList<HV>(byteBuffers.size());
						for (ByteBuffer byteBuffer : byteBuffers) {
							values.add(readHashValue(byteBuffer));
						}
						return values;
					});
		});
	}

	/* (non-Javadoc)
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

	/* (non-Javadoc)
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

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#keys(java.lang.Object)
	 */
	@Override
	public Mono<List<HK>> keys(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.hKeys(rawKey(key)) //
				.flatMap(Flux::fromIterable) //
				.map(this::readHashKey) //
				.collectList());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.hLen(rawKey(key)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#putAll(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<Boolean> putAll(H key, Map<? extends HK, ? extends HV> map) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(map, "Map must not be null!");

		return createMono(connection -> {

			return Flux.fromIterable(() -> map.entrySet().iterator()) //
					.collectMap(entry -> rawHashKey(entry.getKey()), entry -> rawHashValue(entry.getValue())) //
					.flatMap(serialized -> connection.hMSet(rawKey(key), serialized));

		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#put(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> put(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");
		Assert.notNull(value, "Hash value must not be null!");

		return createMono(connection -> connection.hSet(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#putIfAbsent(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Boolean> putIfAbsent(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashKey, "Hash key must not be null!");
		Assert.notNull(value, "Hash value must not be null!");

		return createMono(connection -> connection.hSetNX(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#values(java.lang.Object)
	 */
	@Override
	public Mono<List<HV>> values(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.hVals(rawKey(key)) //
				.flatMap(Flux::fromIterable) //
				.map(this::readHashValue) //
				.collectList());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#entries(java.lang.Object)
	 */
	@Override
	public Mono<Map<HK, HV>> entries(H key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.hGetAll(rawKey(key)) //
				.map(map -> {

					Map<HK, HV> deserialized = new LinkedHashMap<>(map.size());

					map.forEach((k, v) -> {
						deserialized.put(readHashKey(k), readHashValue(v));
					});

					return deserialized;
				}));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHashOperations#getOperations()
	 */
	@Override
	public ReactiveRedisOperations<H, ?> getOperations() {
		return template;
	}

	private <T> Mono<T> createMono(Function<ReactiveHashCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.hashCommands()));
	}

	private ByteBuffer rawKey(H key) {
		return serialization().key().write(key);
	}

	private ByteBuffer rawHashKey(HK key) {
		return serialization().hashKey().write(key);
	}

	private ByteBuffer rawHashValue(HV key) {
		return serialization().hashValue().write(key);
	}

	@SuppressWarnings("unchecked")
	private HK readHashKey(ByteBuffer value) {
		return (HK) serialization().hashKey().read(value);
	}

	@SuppressWarnings("unchecked")
	private HV readHashValue(ByteBuffer value) {
		return (HV) serialization().hashValue().read(value);
	}

	private ReactiveSerializationContext<H, ?> serialization() {
		return template.serialization();
	}
}
