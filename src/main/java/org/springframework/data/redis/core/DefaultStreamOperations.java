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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
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
import org.springframework.data.redis.support.collections.CollectionUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default implementation of {@link ListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Marcin Zielinski
 * @author John Blum
 * @author jinkshower
 * @since 2.2
 */
@NullUnmarked
class DefaultStreamOperations<K, HK, HV> extends AbstractOperations<K, Object> implements StreamOperations<K, HK, HV> {

	private final StreamObjectMapper objectMapper;

	@SuppressWarnings("unchecked")
	DefaultStreamOperations(@NonNull RedisTemplate<K, ?> template,
			@NonNull HashMapper<? super K, ? super HK, ? super HV> mapper) {

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

	@Override
	public Long acknowledge(@NonNull K key, @NonNull String group, @NonNull String @NonNull... recordIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xAck(rawKey, group, recordIds));
	}

	@Override
	@SuppressWarnings("unchecked")
	public RecordId add(@NonNull Record<K, ?> record) {

		Assert.notNull(record, "Record must not be null");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		ByteRecord binaryRecord = input.serialize(keySerializer(), hashKeySerializer(), hashValueSerializer());

		return execute(connection -> connection.xAdd(binaryRecord));
	}

	@Override
	@SuppressWarnings("unchecked")
	public RecordId add(@NonNull Record<K, ?> record, @NonNull XAddOptions options) {

		Assert.notNull(record, "Record must not be null");
		Assert.notNull(options, "XAddOptions must not be null");

		MapRecord<K, HK, HV> input = StreamObjectMapper.toMapRecord(this, record);

		ByteRecord binaryRecord = input.serialize(keySerializer(), hashKeySerializer(), hashValueSerializer());

		return execute(connection -> connection.streamCommands().xAdd(binaryRecord, options));
	}

	@Override
	public List<MapRecord<K, HK, HV>> claim(@NonNull K key, @NonNull String consumerGroup, @NonNull String newOwner,
			@NonNull XClaimOptions xClaimOptions) {

		return CollectionUtils.nullSafeList(execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.streamCommands().xClaim(rawKey(key), consumerGroup, newOwner, xClaimOptions);
			}
		}));
	}

	@Override
	public Long delete(@NonNull K key, @NonNull RecordId @NonNull... recordIds) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xDel(rawKey, recordIds));
	}

	@Override
	public String createGroup(@NonNull K key, @NonNull ReadOffset readOffset, @NonNull String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupCreate(rawKey, group, readOffset, true));
	}

	@Override
	public Boolean deleteConsumer(@NonNull K key, @NonNull Consumer consumer) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDelConsumer(rawKey, consumer));
	}

	@Override
	public Boolean destroyGroup(@NonNull K key, @NonNull String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xGroupDestroy(rawKey, group));
	}

	@Override
	public XInfoStream info(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfo(rawKey));
	}

	@Override
	public XInfoConsumers consumers(@NonNull K key, @NonNull String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfoConsumers(rawKey, group));
	}

	@Override
	public XInfoGroups groups(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xInfoGroups(rawKey));
	}

	@Override
	public PendingMessages pending(@NonNull K key, @NonNull String group, @NonNull Range<?> range, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, group, range, count));
	}

	@Override
	public PendingMessages pending(@NonNull K key, @NonNull Consumer consumer, @NonNull Range<?> range, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, consumer, range, count));
	}

	@Override
	public PendingMessagesSummary pending(@NonNull K key, @NonNull String group) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xPending(rawKey, group));
	}

	@Override
	public Long size(@NonNull K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xLen(rawKey));
	}

	@Override
	public List<MapRecord<K, HK, HV>> range(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRange(rawKey(key), range, limit);
			}
		});
	}

	@Override
	public List<MapRecord<K, HK, HV>> read(@NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<K> @NonNull... streams) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRead(readOptions, rawStreamOffsets(streams));
			}
		});
	}

	@Override
	public List<MapRecord<K, HK, HV>> read(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<K> @NonNull... streams) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xReadGroup(consumer, readOptions, rawStreamOffsets(streams));
			}
		});
	}

	@Override
	public List<MapRecord<K, HK, HV>> reverseRange(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit) {

		return execute(new RecordDeserializingRedisCallback() {

			@Nullable
			@Override
			List<ByteRecord> inRedis(RedisConnection connection) {
				return connection.xRevRange(rawKey(key), range, limit);
			}
		});
	}

	@Override
	public Long trim(@NonNull K key, long count) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count));
	}

	@Override
	public Long trim(@NonNull K key, long count, boolean approximateTrimming) {
		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.xTrim(rawKey, count, approximateTrimming));
	}

	@Override
	public <V> HashMapper<V, HK, HV> getHashMapper(@NonNull Class<V> targetType) {
		return objectMapper.getHashMapper(targetType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public MapRecord<K, HK, HV> deserializeRecord(@NonNull ByteRecord record) {
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
				.toArray(StreamOffset[]::new);
	}

	abstract class RecordDeserializingRedisCallback implements RedisCallback<List<MapRecord<K, HK, HV>>> {

		public final List<MapRecord<K, HK, HV>> doInRedis(@NonNull RedisConnection connection) {

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

		abstract List<ByteRecord> inRedis(RedisConnection connection);
	}

}
