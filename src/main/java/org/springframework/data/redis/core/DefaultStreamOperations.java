/*
 * Copyright 2018-2021 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default implementation of {@link ListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
class DefaultStreamOperations<K, HK, HV> extends AbstractOperations<K, Object> implements StreamOperations<K, HK, HV> {

	private final StreamObjectMapper objectMapper;

	@SuppressWarnings("unchecked")
	DefaultStreamOperations(RedisTemplate<K, ?> template,
			@Nullable HashMapper<? super K, ? super HK, ? super HV> mapper) {

		super((RedisTemplate<K, Object>) template);

		this.objectMapper = new StreamObjectMapper(mapper) {
			@Override
			protected HashMapper<?, ?, ?> doGetHashMapper(ConversionService conversionService, Class<?> targetType) {

				if (isSimpleType(targetType)) {

					return new HashMapper<Object, Object, Object>() {

						@Override
						public Map<Object, Object> toHash(Object object) {

							Object key = "payload";
							Object value = object;

							if (!template.isEnableDefaultSerializer()) {
								if (template.getHashKeySerializer() == null) {
									key = key.toString().getBytes(StandardCharsets.UTF_8);
								}
								if (template.getHashValueSerializer() == null) {
									value = serializeHashValueIfRequires((HV) object);
								}
							}

							return Collections.singletonMap(key, value);
						}

						@Override
						public Object fromHash(Map<Object, Object> hash) {
							Object value = hash.values().iterator().next();
							if (ClassUtils.isAssignableValue(targetType, value)) {
								return value;
							}

							HV deserialized = deserializeHashValue((byte[]) value);

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
	 * @see org.springframework.data.redis.core.StreamOperations#acknowledge(java.lang.Object, java.lang.String, java.lang.String[])
	 */
	@Override
	public Long acknowledge(K key, String group, String... recordIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xAck(rawKey, group, recordIds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#add(org.springframework.data.redis.connection.stream.Record)
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public RecordId add(Record<K, ?> record) {

		Assert.notNull(record, "Record must not be null");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		ByteRecord binaryRecord = input.serialize(keySerializer(), hashKeySerializer(), hashValueSerializer());

		return execute(connection -> connection.xAdd(binaryRecord), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#delete(java.lang.Object, java.lang.String[])
	 */
	@Override
	public Long delete(K key, RecordId... recordIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xDel(rawKey, recordIds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#createGroup(java.lang.Object, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String createGroup(K key, ReadOffset readOffset, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupCreate(rawKey, group, readOffset, true), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#deleteConsumer(java.lang.Object, org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean deleteConsumer(K key, Consumer consumer) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDelConsumer(rawKey, consumer), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#destroyGroup(java.lang.Object, java.lang.String)
	 */
	@Override
	public Boolean destroyGroup(K key, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDestroy(rawKey, group), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#info(java.lang.Object)
	 */
	@Override
	public XInfoStream info(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfo(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#consumers(java.lang.Object, java.lang.String)
	 */
	@Override
	public XInfoConsumers consumers(K key, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfoConsumers(rawKey, group), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#groups(java.lang.Object)
	 */
	@Override
	public XInfoGroups groups(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfoGroups(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, java.lang.String, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public PendingMessages pending(K key, String group, Range<?> range, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, group, range, count, null), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, java.lang.String, org.springframework.data.domain.Range, java.lang.Long, java.lang.Long)
	 */
	@Override
	public PendingMessages pending(K key, String group, Range<?> range, long count,
			long idleMilliSeconds) {
		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, group, range, count, idleMilliSeconds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, org.springframework.data.redis.connection.stream.Consumer, org.springframework.data.domain.Range, java.lang.Long)
	 */
	@Override
	public PendingMessages pending(K key, Consumer consumer, Range<?> range, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, consumer, range, count, null), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, org.springframework.data.redis.connection.stream.Consumer, org.springframework.data.domain.Range, java.lang.Long, java.lang.Long)
	 */
	@Override
	public PendingMessages pending(K key, Consumer consumer, Range<?> range, long count,
			long idleMilliSeconds) {
		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, consumer, range, count, idleMilliSeconds), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#pending(java.lang.Object, java.lang.String)
	 */
	@Override
	public PendingMessagesSummary pending(K key, String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, group), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xLen(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#range(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRange(rawKey(key), range, limit);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRead(readOptions, rawStreamOffsets(streams));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xReadGroup(consumer, readOptions, rawStreamOffsets(streams));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.StreamOperations#reverseRange(java.lang.Object, org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRevRange(rawKey(key), range, limit);
			}
		}, true);
	}

	@Override
	public Long trim(K key, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count), true);
	}

	@Override
	public Long trim(K key, long count, boolean approximateTrimming) {
		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count, approximateTrimming), true);
	}

	@Override
	public <V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType) {
		return objectMapper.getHashMapper(targetType);
	}

	@Override
	public MapRecord<K, HK, HV> deserializeRecord(ByteRecord record) {
		return record.deserialize(keySerializer(), hashKeySerializer(), hashValueSerializer());
	}

	protected byte[] serializeHashValueIfRequires(HV value) {
		return hashValueSerializerPresent() ? serialize(value, hashValueSerializer())
				: objectMapper.getConversionService().convert(value, byte[].class);
	}

	protected boolean hashValueSerializerPresent() {
		return hashValueSerializer() != null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private byte[] serialize(Object value, RedisSerializer serializer) {

		Object _value = value;
		if (!serializer.canSerialize(value.getClass())) {
			_value = objectMapper.getConversionService().convert(value, serializer.getTargetType());
		}
		return serializer.serialize(_value);
	}

	@SuppressWarnings("unchecked")
	private StreamOffset<byte[]>[] rawStreamOffsets(StreamOffset<K>[] streams) {

		return Arrays.stream(streams) //
				.map(it -> StreamOffset.create(rawKey(it.getKey()), it.getOffset())) //
				.toArray(it -> new StreamOffset[it]);
	}

	abstract class RecordDeserializingRedisCallback implements RedisCallback<List<MapRecord<K, HK, HV>>> {

		@SuppressWarnings("unchecked")
		public final List<MapRecord<K, HK, HV>> doInRedis(RedisConnection connection) {

			List<ByteRecord> raw = inRedis(connection);
			if (raw == null) {
				return Collections.emptyList();
			}

			List<MapRecord<K, HK, HV>> result = new ArrayList<>();
			for (ByteRecord record : raw) {
				result.add(deserializeRecord(record));
			}

			return result;
		}

		@Nullable
		abstract List<ByteRecord> inRedis(RedisConnection connection);
	}
}
