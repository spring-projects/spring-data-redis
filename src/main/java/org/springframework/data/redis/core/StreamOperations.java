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

import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
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
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis stream specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Dengliming
 * @since 2.2
 */
public interface StreamOperations<K, HK, HV> extends HashMapperProvider<HK, HV> {

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record id's to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long acknowledge(K key, String group, String... recordIds);

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record id's to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	default Long acknowledge(K key, String group, RecordId... recordIds) {
		return acknowledge(key, group, Arrays.stream(recordIds).map(RecordId::getValue).toArray(String[]::new));
	}

	/**
	 * Acknowledge the given record as processed.
	 *
	 * @param group name of the consumer group.
	 * @param record the {@link Record} to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	default Long acknowledge(String group, Record<K, ?> record) {
		return acknowledge(record.getStream(), group, record.getId());
	}

	/**
	 * Append a record to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param content record content as Map.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	default RecordId add(K key, Map<? extends HK, ? extends HV> content) {
		return add(StreamRecords.newRecord().in(key).ofMap(content));
	}

	/**
	 * Append a record, backed by a {@link Map} holding the field/value pairs, to the stream.
	 *
	 * @param record the record to append.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	default RecordId add(MapRecord<K, ? extends HK, ? extends HV> record) {
		return add((Record) record);
	}

	/**
	 * Append the record, backed by the given value, to the stream. The value is mapped as hash and serialized.
	 *
	 * @param record must not be {@literal null}.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see MapRecord
	 * @see ObjectRecord
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	RecordId add(Record<K, ?> record);

	/**
	 * Removes the specified records from the stream. Returns the number of records deleted, that may be different from
	 * the number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	default Long delete(K key, String... recordIds) {
		return delete(key, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Removes a given {@link Record} from the stream.
	 *
	 * @param record must not be {@literal null}.
	 * @return he {@link Mono} emitting the number of removed records.
	 */
	@Nullable
	default Long delete(Record<K, ?> record) {
		return delete(record.getStream(), record.getId());
	}

	/**
	 * Removes the specified records from the stream. Returns the number of records deleted, that may be different from
	 * the number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return the {@link Mono} emitting the number of removed records.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	Long delete(K key, RecordId... recordIds);

	/**
	 * Create a consumer group at the {@link ReadOffset#latest() latest offset}. This command creates the stream if it
	 * does not already exist.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the consumer group.
	 * @return {@literal OK} if successful. {@literal null} when used in pipeline / transaction.
	 */
	default String createGroup(K key, String group) {
		return createGroup(key, ReadOffset.latest(), group);
	}

	/**
	 * Create a consumer group. This command creates the stream if it does not already exist.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param readOffset the {@link ReadOffset} to apply.
	 * @param group name of the consumer group.
	 * @return {@literal OK} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String createGroup(K key, ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean deleteConsumer(K key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean destroyGroup(K key, String group);

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the {@literal consumer group}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoConsumers consumers(K key, String group);

	/**
	 * Obtain information about {@literal consumer groups} associated with the stream stored at the specified
	 * {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoGroups groups(K key);

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoStream info(K key);

	/**
	 * Obtain the {@link PendingMessagesSummary} for a given {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param group the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @return a summary of pending messages within the given {@literal consumer group} or {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	PendingMessagesSummary pending(K key, String group);

	/**
	 * Obtained detailed information about all pending messages for a given {@link Consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default PendingMessages pending(K key, Consumer consumer) {
		return pending(key, consumer, Range.unbounded(), -1L);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} within a
	 * {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param group the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages pending(K key, String group, Range<?> range, long count);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} within a
	 * {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param group the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results.
	 * @param idleMilliSeconds the time limit until the pending messages are idle. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages pending(K key, String group, Range<?> range, long count, long idleMilliSeconds);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages pending(K key, Consumer consumer, Range<?> range, long count);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results.
	 * @param idleMilliSeconds the time limit until the pending messages are idle. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages pending(K key, Consumer consumer, Range<?> range, long count, long idleMilliSeconds);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long size(K key);

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> range(K key, Range<String> range) {
		return range(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit);

	/**
	 * Read all records from a stream within a specific {@link Range} as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default <V> List<ObjectRecord<K, V>> range(Class<V> targetType, K key, Range<String> range) {
		return range(targetType, key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default <V> List<ObjectRecord<K, V>> range(Class<V> targetType, K key, Range<String> range, Limit limit) {

		Assert.notNull(targetType, "Target type must not be null");

		return map(range(key, range, limit), targetType);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(StreamOffset<K>... streams) {
		return read(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default <V> List<ObjectRecord<K, V>> read(Class<V> targetType, StreamOffset<K>... streams) {
		return read(targetType, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default <V> List<ObjectRecord<K, V>> read(Class<V> targetType, StreamReadOptions readOptions,
			StreamOffset<K>... streams) {

		Assert.notNull(targetType, "Target type must not be null");

		return map(read(readOptions, streams), targetType);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K>... streams) {
		return read(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default <V> List<ObjectRecord<K, V>> read(Class<V> targetType, Consumer consumer, StreamOffset<K>... streams) {
		return read(targetType, consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default <V> List<ObjectRecord<K, V>> read(Class<V> targetType, Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<K>... streams) {

		Assert.notNull(targetType, "Target type must not be null");

		return map(read(consumer, readOptions, streams), targetType);
	}

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range) {
		return reverseRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order as {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default <V> List<ObjectRecord<K, V>> reverseRange(Class<V> targetType, K key, Range<String> range) {
		return reverseRange(targetType, key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order as
	 * {@link ObjectRecord}.
	 *
	 * @param targetType the target type of the payload.
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default <V> List<ObjectRecord<K, V>> reverseRange(Class<V> targetType, K key, Range<String> range, Limit limit) {

		Assert.notNull(targetType, "Target type must not be null");

		return map(reverseRange(key, range, limit), targetType);
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long trim(K key, long count);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @param approximateTrimming the trimming must be performed in a approximated way in order to maximize performances.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long trim(K key, long count, boolean approximateTrimming);

	/**
	 * Get the {@link HashMapper} for a specific type.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param <V>
	 * @return the {@link HashMapper} suitable for a given type;
	 */
	@Override
	<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);

	/**
	 * Map record from {@link MapRecord} to {@link ObjectRecord}.
	 *
	 * @param record the stream record to map.
	 * @param targetType the target type of the payload.
	 * @return the mapped {@link ObjectRecord}.
	 * @since 2.x
	 */
	default <V> ObjectRecord<K, V> map(MapRecord<K, HK, HV> record, Class<V> targetType) {

		Assert.notNull(record, "Record must not be null");
		Assert.notNull(targetType, "Target type must not be null");

		return StreamObjectMapper.toObjectRecord(record, this, targetType);
	}

	/**
	 * Map records from {@link MapRecord} to {@link ObjectRecord}s.
	 *
	 * @param records the stream records to map.
	 * @param targetType the target type of the payload.
	 * @return the mapped {@link ObjectRecord object records}.
	 * @since 2.x
	 */
	@Nullable
	default <V> List<ObjectRecord<K, V>> map(@Nullable List<MapRecord<K, HK, HV>> records, Class<V> targetType) {

		Assert.notNull(records, "Records must not be null");
		Assert.notNull(targetType, "Target type must not be null");

		return StreamObjectMapper.toObjectRecords(records, this, targetType);
	}

	/**
	 * Deserialize a {@link ByteRecord} using the configured serializers into a {@link MapRecord}.
	 *
	 * @param record the stream record to map.
	 * @return deserialized {@link MapRecord}.
	 * @since 2.x
	 */
	MapRecord<K, HK, HV> deserializeRecord(ByteRecord record);
}
