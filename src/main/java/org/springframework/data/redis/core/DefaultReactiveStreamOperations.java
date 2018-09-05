/*
 * Copyright 2018 the original author or authors.
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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveStreamOperations}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
@RequiredArgsConstructor
class DefaultReactiveStreamOperations<K, V> implements ReactiveStreamOperations<K, V> {

	private final @NonNull ReactiveRedisTemplate<?, ?> template;
	private final @NonNull RedisSerializationContext<K, V> serializationContext;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(java.lang.Object, java.lang.String, java.lang.String[])
	 */
	@Override
	public Mono<Long> acknowledge(K key, String group, String... messageIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group must not be null or empty!");
		Assert.notNull(messageIds, "MessageIds must not be null!");
		Assert.notEmpty(messageIds, "MessageIds must not be empty!");

		return createMono(connection -> connection.xAck(rawKey(key), group, messageIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#add(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<String> add(K key, Map<K, V> body) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(body, "Body must not be null!");
		Assert.isTrue(!body.isEmpty(), "Body must not be empty!");

		Map<ByteBuffer, ByteBuffer> rawBody = new LinkedHashMap<>(body.size());

		body.forEach((k, v) -> rawBody.put(rawKey(k), rawValue(v)));

		return createMono(connection -> connection.xAdd(rawKey(key), rawBody));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#delete(java.lang.Object, java.lang.String[])
	 */
	@Override
	public Mono<Long> delete(K key, String... messageIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(messageIds, "MessageIds must not be null!");

		return createMono(connection -> connection.xDel(rawKey(key), messageIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#size(java.lang.Object)
	 */
	@Override
	public Mono<Long> size(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.xLen(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#range(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Flux<StreamMessage<K, V>> range(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRange(rawKey(key), range, limit).map(this::readValue));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<StreamMessage<K, V>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xRead(readOptions, streamOffsets).map(this::readValue);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<StreamMessage<K, V>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xReadGroup(consumer, readOptions, streamOffsets).map(this::readValue);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Flux<StreamMessage<K, V>> reverseRange(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRevRange(rawKey(key), range, limit).map(this::readValue));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#trim(java.lang.Object, long)
	 */
	@Override
	public Mono<Long> trim(K key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.xTrim(rawKey(key), count));
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<ByteBuffer>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams).map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset()))
				.toArray(StreamOffset[]::new);
	}

	private StreamMessage<K, V> readValue(StreamMessage<ByteBuffer, ByteBuffer> message) {

		Map<K, V> body = new LinkedHashMap<>(message.getBody().size());

		message.getBody().forEach((k, v) -> body.put(readKey(k), readValue(v)));

		return new StreamMessage<>(readKey(message.getStream()), message.getId(), body);
	}

	private <T> Mono<T> createMono(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.streamCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createFlux(connection -> function.apply(connection.streamCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawValue(V value) {
		return serializationContext.getValueSerializationPair().write(value);
	}

	private K readKey(ByteBuffer buffer) {
		return serializationContext.getKeySerializationPair().read(buffer);
	}

	private V readValue(ByteBuffer buffer) {
		return serializationContext.getValueSerializationPair().read(buffer);
	}
}
