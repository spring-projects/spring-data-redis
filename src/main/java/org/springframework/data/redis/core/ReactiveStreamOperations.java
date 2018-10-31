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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.StreamRecords;
import org.springframework.data.redis.hash.HashMapper;

/**
 * Redis stream specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public interface ReactiveStreamOperations<K, HK, HV> {

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record Id's to acknowledge.
	 * @return the {@link Mono} emitting the length of acknowledged records.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	default Mono<Long> acknowledge(K key, String group, String... recordIds) {
		return acknowledge(key, group, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record Id's to acknowledge.
	 * @return the {@link Mono} emitting the length of acknowledged records.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	Mono<Long> acknowledge(K key, String group, RecordId... recordIds);

	/**
	 * Acknowledge the given record as processed.
	 *
	 * @param group name of the consumer group.
	 * @param record the {@link Record} to acknowledge.
	 * @return the {@link Mono} emitting the length of acknowledged records.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	default Mono<Long> acknowledge(String group, Record<K, ?> record) {
		return acknowledge(record.getStream(), group, record.getId());
	}

	/**
	 * Append one or more records to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param bodyPublisher record body {@link Publisher}.
	 * @return the record Ids.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Flux<RecordId> add(K key, Publisher<? extends Map<HK, HV>> bodyPublisher) {
		return Flux.from(bodyPublisher).flatMap(it -> add(key, it));
	}

	/**
	 * Append a record to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param body record body.
	 * @return the {@link Mono} emitting the {@link RecordId}.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<RecordId> add(K key, Map<HK, HV> body) {
		return add(StreamRecords.newRecord().in(key).ofMap(body));
	}

	/**
	 * Append a record, backed by a {@link Map} holding the field/value pairs, to the stream.
	 *
	 * @param record the record to append.
	 * @return the {@link Mono} emitting the {@link RecordId}.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	Mono<RecordId> add(MapRecord<K, HK, HV> record);

	/**
	 * Append the record, backed by the given value, to the stream. The value will be hashed and serialized.
	 *
	 * @param record must not be {@literal null}.
	 * @param <V>
	 * @return
	 */
	default <V> Mono<RecordId> add(Record<K, V> record) {
		return add(toMapRecord(record));
	}

	/**
	 * Removes the specified records from the stream. Returns the number of records deleted, that may be different from
	 * the number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return the {@link Mono} emitting the number of removed records.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> delete(K key, String... recordIds) {
		return delete(key, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Removes a given {@link Record} from the stream.
	 *
	 * @param record must not be {@literal null}.
	 * @return he {@link Mono} emitting the number of removed records.
	 */
	default Mono<Long> delete(Record<K, ?> record) {
		return delete(record.getStream(), record.getId());
	}

	/**
	 * Removes the specified records from the stream. Returns the number of records deleted, that may be different from
	 * the number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return the {@link Mono} emitting the number of removed records.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	Mono<Long> delete(K key, RecordId... recordIds);

	/**
	 * Create a consumer group at the {@link ReadOffset#latest() latest offset}.
	 *
	 * @param key
	 * @param group name of the consumer group.
	 * @return the {@link Mono} emitting {@literal ok} if successful.. {@literal null} when used in pipeline /
	 *         transaction.
	 */
	default Mono<String> createGroup(K key, String group) {
		return createGroup(key, ReadOffset.latest(), group);
	}

	/**
	 * Create a consumer group.
	 *
	 * @param key
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 */
	Mono<String> createGroup(K key, ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @return the {@link Mono} {@literal ok} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Mono<String> deleteConsumer(K key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return the {@link Mono} {@literal ok} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Mono<String> destroyGroup(K key, String group);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return the {@link Mono} emitting the length of the stream.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return the {@link Flux} emitting the records one by one.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range) {
		return range(key, range, Limit.unlimited());
	}

	/**
	 * Read all records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return lthe {@link Flux} emitting the records one by one.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> range(K key, Range<String> range, Class<V> targetType) {
		return range(key, range, Limit.unlimited(), targetType);
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return lthe {@link Flux} emitting the records one by one.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	Flux<MapRecord<K, HK, HV>> range(K key, Range<String> range, Limit limit);

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return lthe {@link Flux} emitting the records one by one.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> range(K key, Range<String> range, Limit limit, Class<V> targetType) {
		return range(key, range, limit).map(it -> toObjectRecord(it, targetType));
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param targetType
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> read(Class<V> targetType, StreamOffset<K>... streams) {
		return read(targetType, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(StreamOffset<K>... streams) {
		return read(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	Flux<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @oaram targetType
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> read(Class<V> targetType, StreamReadOptions readOptions,
			StreamOffset<K>... streams) {

		return read(readOptions, streams).map(it -> toObjectRecord(it, targetType));
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamOffset<K>... streams) {
		return read(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> read(Class<V> targetType, Consumer consumer, StreamOffset<K>... streams) {
		return read(targetType, consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	Flux<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<K>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param targetType
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> read(Class<V> targetType, Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<K>... streams) {
		return read(consumer, readOptions, streams).map(it -> toObjectRecord(it, targetType));
	}

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range) {
		return reverseRange(key, range, Limit.unlimited());
	}

	default <V> Flux<ObjectRecord<K, V>> reverseRange(Class<V> targetType, K key, Range<String> range) {
		return reverseRange(targetType, key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	Flux<MapRecord<K, HK, HV>> reverseRange(K key, Range<String> range, Limit limit);

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param targetType
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default <V> Flux<ObjectRecord<K, V>> reverseRange(Class<V> targetType, K key, Range<String> range, Limit limit) {
		return reverseRange(key, range, limit).map(it -> toObjectRecord(it, targetType));
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Mono<Long> trim(K key, long count);

	/**
	 * Get the {@link HashMapper} for a specific type.
	 *
	 * @param targetType must not be {@literal null}.
	 * @param <V>
	 * @return the {@link HashMapper} suitable for a given type;
	 */
	<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);

	/**
	 * App
	 *
	 * @param value
	 * @param <V>
	 * @return
	 */
	default <V> MapRecord<K, HK, HV> toMapRecord(Record<K, V> value) {

		if (value instanceof ObjectRecord) {

			ObjectRecord entry = ((ObjectRecord) value);

			// TODO: should we have this?
			if (entry.getValue() instanceof Map) {
				return StreamRecords.newRecord().in(value.getStream()).withId(value.getId()).ofMap((Map) entry.getValue());
			}

			return entry.toMapRecord(getHashMapper(entry.getValue().getClass()));
		}

		if (value instanceof MapRecord) {
			return (MapRecord<K, HK, HV>) value;
		}

		return Record.of(((HashMapper) getHashMapper(value.getClass())).toHash(value)).withStreamKey(value.getStream());
	}

	default <V> ObjectRecord<K, V> toObjectRecord(MapRecord<K, HK, HV> entry, Class<V> targetType) {
		return entry.toObjectRecord(getHashMapper(targetType));
	}
}
