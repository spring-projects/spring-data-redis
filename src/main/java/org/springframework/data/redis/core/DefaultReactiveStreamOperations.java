/*
 * Copyright 2018-2020 the original author or authors.
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.ByteBufferRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumer;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroup;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default implementation of {@link ReactiveStreamOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tugdual Grall
 * @since 2.2
 */
class DefaultReactiveStreamOperations<K, HK, HV> implements ReactiveStreamOperations<K, HK, HV> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, ?> serializationContext;
	private final StreamObjectMapper objectMapper;

	DefaultReactiveStreamOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<K, ?> serializationContext,
			@Nullable HashMapper<? super K, ? super HK, ? super HV> hashMapper) {

		this.template = template;
		this.serializationContext = serializationContext;
		this.objectMapper = new StreamObjectMapper(hashMapper) {

			@Override
			protected HashMapper<?, ?, ?> doGetHashMapper(ConversionService conversionService, Class<?> targetType) {

				if (objectMapper.isSimpleType(targetType) || ClassUtils.isAssignable(ByteBuffer.class, targetType)) {

					return new HashMapper<Object, Object, Object>() {

						@Override
						public Map<Object, Object> toHash(Object object) {

							Object key = "payload";
							Object value = object;

							if (serializationContext.getHashKeySerializationPair() == null) {
								key = key.toString().getBytes(StandardCharsets.UTF_8);
							}
							if (serializationContext.getHashValueSerializationPair() == null) {
								value = conversionService.convert(value, byte[].class);
							}

							return Collections.singletonMap(key, value);
						}

						@Override
						public Object fromHash(Map<Object, Object> hash) {
							Object value = hash.values().iterator().next();
							if (ClassUtils.isAssignableValue(targetType, value)) {
								return value;
							}

							HV deserialized = deserializeHashValue((ByteBuffer) value);

							if (ClassUtils.isAssignableValue(targetType, deserialized)) {
								return value;
							}

							return conversionService.convert(deserialized, targetType);
						}
					};
				}

				return super.doGetHashMapper(conversionService, targetType);
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(java.lang.Object, java.lang.String, java.lang.String[])
	 */
	@Override
	public Mono<Long> acknowledge(K key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group must not be null or empty!");
		Assert.notNull(recordIds, "MessageIds must not be null!");
		Assert.notEmpty(recordIds, "MessageIds must not be empty!");

		return createMono(connection -> connection.xAck(rawKey(key), group, recordIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#add(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<RecordId> add(Record<K, ?> record) {

		Assert.notNull(record.getStream(), "Key must not be null!");
		Assert.notNull(record.getValue(), "Body must not be null!");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		return createMono(connection -> connection.xAdd(serializeRecord(input)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#delete(java.lang.Object, java.lang.String[])
	 */
	@Override
	public Mono<Long> delete(K key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "MessageIds must not be null!");

		return createMono(connection -> connection.xDel(rawKey(key), recordIds));
	}

	@Override
	public Mono<String> createGroup(K key, ReadOffset readOffset, String group) {
		return createMono(connection -> connection.xGroupCreate(rawKey(key), group, readOffset, false));
	}

	@Override
	public Mono<String> createGroup(K key, ReadOffset readOffset, String group, boolean mkStream) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");
		Assert.notNull(group, "Group must not be null!");

		return createMono(connection -> connection.xGroupCreate(rawKey(key), group, readOffset, mkStream));
	}

	@Override
	public Mono<String> deleteConsumer(K key, Consumer consumer) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(consumer, "Consumer must not be null!");

		return createMono(connection -> connection.xGroupDelConsumer(rawKey(key), consumer));
	}

	@Override
	public Mono<String> destroyGroup(K key, String group) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(group, "Group must not be null!");

		return createMono(connection -> connection.xGroupDestroy(rawKey(key), group));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#consumers(java.lang.Object)
	 */
	@Override
	public Flux<XInfoConsumer> consumers(K key, String group) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(group, "Group must not be null!");

		return createFlux(connection -> connection.xInfoConsumers(rawKey(key), group));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#info(java.lang.Object)
	 */
	@Override
	public Mono<XInfoStream> info(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> connection.xInfo(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#groups(java.lang.Object)
	 */
	@Override
	public Flux<XInfoGroup> groups(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createFlux(connection -> connection.xInfoGroups(rawKey(key)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, java.lang.String, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public Mono<PendingMessages> pending(K key, String group, Range<?> range, long count) {

		ByteBuffer rawKey = rawKey(key);
		return createMono(connection -> connection.xPending(rawKey, group, range, count));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, org.springframework.data.redis.connection.stream.Consumer, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public Mono<PendingMessages> pending(K key, Consumer consumer, Range<?> range, long count) {

		ByteBuffer rawKey = rawKey(key);
		return createMono(connection -> connection.xPending(rawKey, consumer, range, count));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, java.lang.String)
	 */
	@Override
	public Mono<PendingMessagesSummary> pending(K key, String group) {

		ByteBuffer rawKey = rawKey(key);
		return createMono(connection -> connection.xPending(rawKey, group));
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
	public Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRange(rawKey(key), range, limit).map(this::deserializeRecord));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xRead(readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "Streams must not be null!");

		return createFlux(connection -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return connection.xReadGroup(consumer, readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveStreamOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return createFlux(connection -> connection.xRevRange(rawKey(key), range, limit).map(this::deserializeRecord));
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

	@Override
	public <V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType) {
		return objectMapper.getHashMapper(targetType);
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<ByteBuffer>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams).map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset()))
				.toArray(StreamOffset[]::new);
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

	@SuppressWarnings("unchecked")
	private ByteBuffer rawHashKey(HK key) {
		try {
			return serializationContext.getHashKeySerializationPair().write(key);
		} catch (IllegalStateException e) {}
		return ByteBuffer.wrap(objectMapper.getConversionService().convert(key, byte[].class));
	}

	@SuppressWarnings("unchecked")
	private ByteBuffer rawValue(HV value) {

		try {
			return serializationContext.getHashValueSerializationPair().write(value);
		} catch (IllegalStateException e) {}
		return ByteBuffer.wrap(objectMapper.getConversionService().convert(value, byte[].class));
	}

	@SuppressWarnings("unchecked")
	private HK readHashKey(ByteBuffer buffer) {
		return (HK) serializationContext.getHashKeySerializationPair().getReader().read(buffer);
	}

	private K readKey(ByteBuffer buffer) {
		return serializationContext.getKeySerializationPair().read(buffer);
	}

	@SuppressWarnings("unchecked")
	private HV deserializeHashValue(ByteBuffer buffer) {
		return (HV) serializationContext.getHashValueSerializationPair().read(buffer);
	}

	private MapRecord<K, HK, HV> deserializeRecord(ByteBufferRecord record) {
		return record.map(it -> it.mapEntries(this::deserializeRecordFields).withStreamKey(readKey(record.getStream())));
	}

	private Entry<HK, HV> deserializeRecordFields(Entry<ByteBuffer, ByteBuffer> it) {
		return Collections.singletonMap(readHashKey(it.getKey()), deserializeHashValue(it.getValue())).entrySet().iterator()
				.next();
	}

	private ByteBufferRecord serializeRecord(MapRecord<K, ? extends HK, ? extends HV> record) {
		return ByteBufferRecord
				.of(record.map(it -> it.mapEntries(this::serializeRecordFields).withStreamKey(rawKey(record.getStream()))));
	}

	private Entry<ByteBuffer, ByteBuffer> serializeRecordFields(Entry<? extends HK, ? extends HV> it) {
		return Collections.singletonMap(rawHashKey(it.getKey()), rawValue(it.getValue())).entrySet().iterator().next();
	}
}
