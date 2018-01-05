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
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveHyperLogLogCommands;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveHyperLogLogOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor
class DefaultReactiveHyperLogLogOperations<K, V> implements ReactiveHyperLogLogOperations<K, V> {

	private final @NonNull ReactiveRedisTemplate<?, ?> template;
	private final @NonNull RedisSerializationContext<K, V> serializationContext;

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHyperLogLogOperations#add(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> add(K key, V... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notEmpty(values, "Values must not be null or empty!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return createMono(connection -> Flux.fromArray(values) //
				.map(this::rawValue) //
				.collectList() //
				.flatMap(serializedValues -> connection.pfAdd(rawKey(key), serializedValues)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHyperLogLogOperations#size(java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> size(K... keys) {

		Assert.notEmpty(keys, "Keys must not be null or empty!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return createMono(connection -> Flux.fromArray(keys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(connection::pfCount));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHyperLogLogOperations#union(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Boolean> union(K destination, K... sourceKeys) {

		Assert.notNull(destination, "Destination key must not be null!");
		Assert.notEmpty(sourceKeys, "Source keys must not be null or empty!");
		Assert.noNullElements(sourceKeys, "Source keys must not contain null elements!");

		return createMono(connection -> Flux.fromArray(sourceKeys) //
				.map(this::rawKey) //
				.collectList() //
				.flatMap(serialized -> connection.pfMerge(rawKey(destination), serialized)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveHyperLogLogOperations#delete(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> delete(K key) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveHyperLogLogCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.hyperLogLogCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.getValueSerializationPair().write(value);
	}
}
