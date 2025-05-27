/*
 * Copyright 2018-2025 the original author or authors.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.reactivestreams.Publisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.convert.Converters;
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
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default implementation of {@link ReactiveStreamOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Marcin Zielinski
 * @author John Blum
 * @author jinkshower
 * @since 2.2
 */
@NullUnmarked
class DefaultReactiveStreamOperations<K, HK, HV> implements ReactiveStreamOperations<K, HK, HV> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<K, ?> serializationContext;
	private final StreamObjectMapper objectMapper;

	DefaultReactiveStreamOperations(@NonNull ReactiveRedisTemplate<?, ?> template,
			@NonNull RedisSerializationContext<K, ?> serializationContext,
			@NonNull HashMapper<? super K, ? super HK, ? super HV> hashMapper) {

		this.template = template;
		this.serializationContext = serializationContext;
		this.objectMapper = new StreamObjectMapper(hashMapper) {

			@Override
			protected HashMapper<?, ?, ?> doGetHashMapper(@NonNull ConversionService conversionService,
					@NonNull Class<?> targetType) {

				if (objectMapper.isSimpleType(targetType) || ClassUtils.isAssignable(ByteBuffer.class, targetType)) {

					return new HashMapper<>() {

						@Override
						public Map<Object, Object> toHash(@NonNull Object object) {

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
						public Object fromHash(@NonNull Map<Object, Object> hash) {

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

	@Override
	public Mono<Long> acknowledge(@NonNull K key, @NonNull String group, RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(group, "Group must not be null or empty");
		Assert.notNull(recordIds, "MessageIds must not be null");
		Assert.notEmpty(recordIds, "MessageIds must not be empty");

		return createMono(streamCommands -> streamCommands.xAck(rawKey(key), group, recordIds));
	}

	@Override
	public Mono<RecordId> add(@NonNull Record<K, ?> record) {

		Assert.notNull(record.getStream(), "Key must not be null");
		Assert.notNull(record.getValue(), "Body must not be null");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		return createMono(streamCommands -> streamCommands.xAdd(serializeRecord(input)));
	}

	@Override
	public Mono<RecordId> add(@NonNull Record<K, ?> record, @NonNull XAddOptions xAddOptions) {

		Assert.notNull(record.getStream(), "Key must not be null");
		Assert.notNull(record.getValue(), "Body must not be null");
		Assert.notNull(xAddOptions, "XAddOptions must not be null");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		return createMono(streamCommands -> streamCommands.xAdd(serializeRecord(input), xAddOptions));
	}

	@Override
	public Flux<MapRecord<K, HK, HV>> claim(@NonNull K key, @NonNull String consumerGroup, @NonNull String newOwner,
			@NonNull XClaimOptions xClaimOptions) {

		return createFlux(streamCommands -> streamCommands.xClaim(rawKey(key), consumerGroup, newOwner, xClaimOptions)
				.map(this::deserializeRecord));
	}

	@Override
	public Mono<Long> delete(@NonNull K key, RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(recordIds, "MessageIds must not be null");

		return createMono(streamCommands -> streamCommands.xDel(rawKey(key), recordIds));
	}

	@Override
	public Mono<String> createGroup(@NonNull K key, @NonNull ReadOffset readOffset, @NonNull String group) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(readOffset, "ReadOffset must not be null");
		Assert.notNull(group, "Group must not be null");

		return createMono(streamCommands -> streamCommands.xGroupCreate(rawKey(key), group, readOffset, true));
	}

	@Override
	public Mono<String> deleteConsumer(@NonNull K key, @NonNull Consumer consumer) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(consumer, "Consumer must not be null");

		return createMono(streamCommands -> streamCommands.xGroupDelConsumer(rawKey(key), consumer));
	}

	@Override
	public Mono<String> destroyGroup(@NonNull K key, @NonNull String group) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(group, "Group must not be null");

		return createMono(streamCommands -> streamCommands.xGroupDestroy(rawKey(key), group));
	}

	@Override
	public Flux<XInfoConsumer> consumers(@NonNull K key, @NonNull String group) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(group, "Group must not be null");

		return createFlux(streamCommands -> streamCommands.xInfoConsumers(rawKey(key), group));
	}

	@Override
	public Mono<XInfoStream> info(@NonNull K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(streamCommands -> streamCommands.xInfo(rawKey(key)));
	}

	@Override
	public Flux<XInfoGroup> groups(@NonNull K key) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(streamCommands -> streamCommands.xInfoGroups(rawKey(key)));
	}

	@Override
	public Mono<PendingMessages> pending(@NonNull K key, @NonNull String group, @NonNull Range<?> range, long count) {

		ByteBuffer rawKey = rawKey(key);

		return createMono(streamCommands -> streamCommands.xPending(rawKey, group, range, count));
	}

	@Override
	public Mono<PendingMessages> pending(@NonNull K key, @NonNull Consumer consumer, @NonNull Range<?> range,
			long count) {

		ByteBuffer rawKey = rawKey(key);

		return createMono(streamCommands -> streamCommands.xPending(rawKey, consumer, range, count));
	}

	@Override
	public Mono<PendingMessagesSummary> pending(@NonNull K key, @NonNull String group) {

		ByteBuffer rawKey = rawKey(key);

		return createMono(streamCommands -> streamCommands.xPending(rawKey, group));
	}

	@Override
	public Mono<Long> size(@NonNull K key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(streamCommands -> streamCommands.xLen(rawKey(key)));
	}

	@Override
	public Flux<MapRecord<K, HK, HV>> range(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return createFlux(streamCommands -> streamCommands.xRange(rawKey(key), range, limit).map(this::deserializeRecord));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<MapRecord<K, HK, HV>> read(@NonNull StreamReadOptions readOptions, StreamOffset<K> @NonNull... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "Streams must not be null");

		return createFlux(streamCommands -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return streamCommands.xRead(readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<MapRecord<K, HK, HV>> read(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			StreamOffset<K> @NonNull... streams) {

		Assert.notNull(consumer, "Consumer must not be null");
		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "Streams must not be null");

		return createFlux(streamCommands -> {

			StreamOffset<ByteBuffer>[] streamOffsets = rawStreamOffsets(streams);

			return streamCommands.xReadGroup(consumer, readOptions, streamOffsets).map(this::deserializeRecord);
		});
	}

	@Override
	public Flux<MapRecord<K, HK, HV>> reverseRange(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return createFlux(
				streamCommands -> streamCommands.xRevRange(rawKey(key), range, limit).map(this::deserializeRecord));
	}

	@Override
	public Mono<Long> trim(@NonNull K key, long count) {
		return trim(key, count, false);
	}

	@Override
	public Mono<Long> trim(@NonNull K key, long count, boolean approximateTrimming) {
		Assert.notNull(key, "Key must not be null");

		return createMono(streamCommands -> streamCommands.xTrim(rawKey(key), count, approximateTrimming));
	}

	@Override
	public <V> HashMapper<V, HK, HV> getHashMapper(@NonNull Class<V> targetType) {
		return objectMapper.getHashMapper(targetType);
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<ByteBuffer>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams).map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset()))
				.toArray(StreamOffset[]::new);
	}

	private <T> Mono<T> createMono(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateMono(connection -> function.apply(connection.streamCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveStreamCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateFlux(connection -> function.apply(connection.streamCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashKey(HK key) {

		try {
			return serializationContext.getHashKeySerializationPair().write(key);
		} catch (IllegalStateException ignore) {}

		return ByteBuffer.wrap(objectMapper.getConversionService().convert(key, byte[].class));
	}

	private ByteBuffer rawValue(HV value) {

		try {
			return serializationContext.getHashValueSerializationPair().write(value);
		} catch (IllegalStateException ignore) {}

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

	@Override
	public MapRecord<K, HK, HV> deserializeRecord(@NonNull ByteBufferRecord record) {
		return record.map(it -> it.mapEntries(this::deserializeRecordFields).withStreamKey(readKey(record.getStream())));
	}

	private Entry<HK, HV> deserializeRecordFields(Entry<ByteBuffer, ByteBuffer> it) {
		return Converters.entryOf(readHashKey(it.getKey()), deserializeHashValue(it.getValue()));
	}

	private ByteBufferRecord serializeRecord(MapRecord<K, ? extends HK, ? extends HV> record) {
		return ByteBufferRecord
				.of(record.map(it -> it.mapEntries(this::serializeRecordFields).withStreamKey(rawKey(record.getStream()))));
	}

	private Entry<ByteBuffer, ByteBuffer> serializeRecordFields(Entry<? extends HK, ? extends HV> it) {
		return Converters.entryOf(rawHashKey(it.getKey()), rawValue(it.getValue()));
	}
}
