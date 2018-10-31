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
package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Stream-specific Redis commands.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see <a href="https://redis.io/topics/streams-intro">Redis Documentation - Streams</a>
 * @since 2.2
 */
public interface RedisStreamCommands {

	/**
	 * Acknowledge one or more records, identified via their id, as processed.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the consumer group.
	 * @param recordIds the String representation of the {@literal id's} of the records to acknowledge.
	 * @return length of acknowledged messages. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	default Long xAck(byte[] key, String group, String... recordIds) {
		return xAck(key, group, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Acknowledge one or more records, identified via their id, as processed.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the consumer group.
	 * @param recordIds the {@literal id's} of the records to acknowledge.
	 * @return length of acknowledged messages. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long xAck(byte[] key, String group, RecordId... recordIds);

	/**
	 * Append a new record with the given {@link Map field/value pairs} as content to the stream stored at {@code key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param content the records content modeled as {@link Map field/value pairs}.
	 * @return the server generated {@link RecordId id}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	default RecordId xAdd(byte[] key, Map<byte[], byte[]> content) {
		return xAdd(StreamRecords.newRecord().in(key).ofMap(content));
	}

	/**
	 * Append the given {@link MapRecord record} to the stream stored at {@link Record#getStream()}. <br />
	 * If you prefer manual id assignment over server generated ones make sure to provide an id via
	 * {@link Record#withId(RecordId)}.
	 *
	 * @param record the {@link MapRecord record} to append.
	 * @return the {@link RecordId id} after save. {@literal null} when used in pipeline / transaction.
	 */
	RecordId xAdd(MapRecord<byte[], byte[], byte[]> record);

	/**
	 * Removes the records with the given id's from the stream. Returns the number of items deleted, that may be different
	 * from the number of id's passed in case certain id's do not exist.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param recordIds the id's of the records to remove.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	default Long xDel(byte[] key, String... recordIds) {
		return xDel(key, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Removes the records with the given id's from the stream. Returns the number of items deleted, that may be different
	 * from the number of id's passed in case certain id's do not exist.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param recordIds the id's of the records to remove.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	Long xDel(byte[] key, RecordId... recordIds);

	/**
	 * Create a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group to create.
	 * @param readOffset the offset to start at.
	 * @return {@literal ok} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName the name of the group to remove the consumer from.
	 * @param consumerName the name of the consumer to remove from the group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	default Boolean xGroupDelConsumer(byte[] key, String groupName, String consumerName) {
		return xGroupDelConsumer(key, Consumer.from(groupName, consumerName));
	}

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param consumer consumer identified by group name and consumer name.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDelConsumer(byte[] key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDestroy(byte[] key, String groupName);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long xLen(byte[] key);

	/**
	 * Retrieve all {@link ByteRecord records} within a specific {@link Range} from the stream stored at
	 * {@literal key}. <br />
	 * Use {@link Range#unbounded()} to read from the minimum and the maximum ID possible.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<ByteRecord> xRange(byte[] key, Range<String> range) {
		return xRange(key, range, Limit.unlimited());
	}

	/**
	 * Retrieve a {@link Limit limited number} of {@link ByteRecord records} within a specific {@link Range} from the
	 * stream stored at {@literal key}. <br />
	 * Use {@link Range#unbounded()} to read from the minimum and the maximum ID possible. <br />
	 * Use {@link Limit#unlimited()} to read all records.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<ByteRecord> xRead(StreamOffset<byte[]>... streams) {
		return xRead(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<ByteRecord> xReadGroup(Consumer consumer, StreamOffset<byte[]>... streams) {
		return xReadGroup(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<ByteRecord> xRevRange(byte[] key, Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long xTrim(byte[] key, long count);

	/**
	 * Value object representing read offset for a Stream.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class ReadOffset {

		private final String offset;

		private ReadOffset(String offset) {
			this.offset = offset;
		}

		/**
		 * Read from the latest offset.
		 *
		 * @return
		 */
		public static ReadOffset latest() {
			return new ReadOffset("$");
		}

		/**
		 * Read all new arriving elements with ids greater than the last one consumed by the consumer group.
		 *
		 * @return the {@link ReadOffset} object without a specific offset.
		 */
		public static ReadOffset lastConsumed() {
			return new ReadOffset(">");
		}

		/**
		 * Read all arriving elements from the stream starting at {@code offset}.
		 *
		 * @param offset the stream offset.
		 * @return the {@link StreamOffset} object without a specific offset.
		 */
		public static ReadOffset from(String offset) {

			Assert.hasText(offset, "Offset must not be empty");

			return new ReadOffset(offset);
		}

		public static ReadOffset from(RecordId offset) {

			if (offset.shouldBeAutoGenerated()) {
				return latest();
			}

			return from(offset.getValue());
		}
	}

	/**
	 * Value object representing a Stream Id with its offset.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class StreamOffset<K> {

		private final K key;
		private final ReadOffset offset;

		private StreamOffset(K key, ReadOffset offset) {
			this.key = key;
			this.offset = offset;
		}

		/**
		 * Create a {@link StreamOffset} given {@code key} and {@link ReadOffset}.
		 *
		 * @return
		 */
		public static <K> StreamOffset<K> create(K key, ReadOffset readOffset) {
			return new StreamOffset<>(key, readOffset);
		}

		public static <K> StreamOffset latest(K key) {
			return new StreamOffset(key, ReadOffset.latest());
		}

		public static <K> StreamOffset<K> of(Record<K, ?> reference) {
			return create(reference.getStream(), ReadOffset.from(reference.getId()));
		}
	}

	/**
	 * Options for reading messages from a Redis Stream.
	 */
	@EqualsAndHashCode
	@ToString
	@Getter
	class StreamReadOptions {

		private static final StreamReadOptions EMPTY = new StreamReadOptions(null, null, false);

		private final @Nullable Long block;
		private final @Nullable Long count;
		private final boolean noack;

		private StreamReadOptions(@Nullable Long block, @Nullable Long count, boolean noack) {
			this.block = block;
			this.count = count;
			this.noack = noack;
		}

		/**
		 * Creates an empty {@link StreamReadOptions} instance.
		 *
		 * @return an empty {@link StreamReadOptions} instance.
		 */
		public static StreamReadOptions empty() {
			return EMPTY;
		}

		/**
		 * Disable auto-acknowledgement when reading in the context of a consumer group.
		 *
		 * @return {@link StreamReadOptions} with {@code noack} applied.
		 */
		public StreamReadOptions noack() {
			return new StreamReadOptions(block, count, true);
		}

		/**
		 * Use a blocking read and supply the {@link Duration timeout} after which the call will terminate if no message was
		 * read.
		 *
		 * @param timeout the timeout for the blocking read, must not be {@literal null} or negative.
		 * @return {@link StreamReadOptions} with {@code block} applied.
		 */
		public StreamReadOptions block(Duration timeout) {

			Assert.notNull(timeout, "Block timeout must not be null!");
			Assert.isTrue(!timeout.isNegative(), "Block timeout must not be negative!");

			return new StreamReadOptions(timeout.toMillis(), count, noack);
		}

		/**
		 * Limit the number of messages returned per stream.
		 *
		 * @param count the maximum number of messages to read.
		 * @return {@link StreamReadOptions} with {@code count} applied.
		 */
		public StreamReadOptions count(long count) {

			Assert.isTrue(count > 0, "Count must be greater or equal to zero!");

			return new StreamReadOptions(block, count, noack);
		}
	}

	/**
	 * Value object representing a Stream consumer within a consumer group. Group name and consumer name are encoded as
	 * keys.
	 */
	@EqualsAndHashCode
	@Getter
	class Consumer {

		private final String group;
		private final String name;

		private Consumer(String group, String name) {
			this.group = group;
			this.name = name;
		}

		/**
		 * Create a new consumer.
		 *
		 * @param group name of the consumer group, must not be {@literal null} or empty.
		 * @param name name of the consumer, must not be {@literal null} or empty.
		 * @return the consumer {@link io.lettuce.core.Consumer} object.
		 */
		public static Consumer from(String group, String name) {

			Assert.hasText(group, "Group must not be null");
			Assert.hasText(name, "Name must not be null");

			return new Consumer(group, name);
		}

		@Override
		public String toString() {
			return String.format("%s:%s", group, name);
		}
	}

	/**
	 * The id of a single {@link Record} within a stream. Composed of two parts:
	 * {@literal <millisecondsTime>-<sequenceNumber>}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/topics/streams-intro#entry-ids">Redis Documentation - Entriy ID</a>
	 */
	@EqualsAndHashCode
	class RecordId {

		private static final String GENERATE_ID = "*";
		private static final String DELIMINATOR = "-";

		/**
		 * Auto-generation of IDs by the server is almost always what you want so we've got this instance here shortcutting
		 * computation.
		 */
		private static final RecordId AUTOGENERATED = new RecordId(GENERATE_ID) {

			@Override
			public Long getSequence() {
				return null;
			}

			@Override
			public Long getTimestamp() {
				return null;
			}

			@Override
			public boolean shouldBeAutoGenerated() {
				return true;
			}
		};

		private final String raw;

		/**
		 * Private constructor - validate input in static initializer blocks.
		 *
		 * @param raw
		 */
		private RecordId(String raw) {
			this.raw = raw;
		}

		/**
		 * Obtain an instance of {@link RecordId} using the provided String formatted as
		 * {@literal <millisecondsTime>-<sequenceNumber>}. <br />
		 * For server auto generated {@literal entry-id} on insert pass in {@literal null} or {@literal *}. Event better,
		 * just use {@link #autoGenerate()}.
		 *
		 * @param value can be {@literal null}.
		 * @return new instance of {@link RecordId} if no autogenerated one requested.
		 */
		public static RecordId of(@Nullable String value) {

			if (value == null || GENERATE_ID.equals(value)) {
				return autoGenerate();
			}

			Assert.isTrue(value.contains(DELIMINATOR),
					"Invalid id format. Please use the 'millisecondsTime-sequenceNumber' format.");
			return new RecordId(value);
		}

		/**
		 * Create a new instance of {@link RecordId} using the provided String formatted as
		 * {@literal <millisecondsTime>-<sequenceNumber>}. <br />
		 * For server auto generated {@literal entry-id} on insert use {@link #autoGenerate()}.
		 *
		 * @param millisecondsTime
		 * @param sequenceNumber
		 * @return new instance of {@link RecordId}.
		 */
		public static RecordId of(long millisecondsTime, long sequenceNumber) {
			return of(millisecondsTime + DELIMINATOR + sequenceNumber);
		}

		/**
		 * Obtain the {@link RecordId} signalling the server to auto generate an {@literal entry-id} on insert
		 * ({@code XADD}).
		 *
		 * @return {@link RecordId} instance signalling {@link #shouldBeAutoGenerated()}.
		 */
		public static RecordId autoGenerate() {
			return AUTOGENERATED;
		}

		/**
		 * Get the {@literal entry-id millisecondsTime} part or {@literal null} if it {@link #shouldBeAutoGenerated()}.
		 *
		 * @return millisecondsTime of the {@literal entry-id}. Can be {@literal null}.
		 */
		@Nullable
		public Long getTimestamp() {
			return value(0);
		}

		/**
		 * Get the {@literal entry-id sequenceNumber} part or {@literal null} if it {@link #shouldBeAutoGenerated()}.
		 *
		 * @return sequenceNumber of the {@literal entry-id}. Can be {@literal null}.
		 */
		@Nullable
		public Long getSequence() {
			return value(1);
		}

		/**
		 * @return {@literal true} if a new {@literal entry-id} shall be generated on server side when calling {@code XADD}.
		 */
		public boolean shouldBeAutoGenerated() {
			return false;
		}

		/**
		 * @return get the string representation of the {@literal entry-id} in
		 *         {@literal <millisecondsTime>-<sequenceNumber>} format or {@literal *} if it
		 *         {@link #shouldBeAutoGenerated()}. Never {@literal null}.
		 */
		public String getValue() {
			return raw;
		}

		@Override
		public String toString() {
			return raw;
		}

		private Long value(int index) {
			return NumberUtils.parseNumber(StringUtils.split(raw, DELIMINATOR)[index], Long.class);
		}
	}

	/**
	 * A single entry in the stream consisting of the {@link RecordId entry-id} and the actual entry-value (typically a
	 * collection of {@link MapRecord field/value pairs}).
	 *
	 * @param <V> the type backing the {@link Record}.
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/topics/streams-intro#streams-basics">Redis Documentation - Stream Basics</a>
	 */
	interface Record<S, V> {

		/**
		 * The id of the stream (aka the {@literal key} in Redis).
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		S getStream();

		/**
		 * The id of the entry inside the stream.
		 *
		 * @return never {@literal null}.
		 */
		RecordId getId();

		/**
		 * @return the actual content. Never {@literal null}.
		 */
		V getValue();

		/**
		 * Create a new {@link MapRecord} instance backed by the given {@link Map} holding {@literal field/value} pairs.
		 * <br />
		 * You may want to use the builders available via {@link StreamRecords}.
		 *
		 * @param map the raw map.
		 * @param <K> the key type of the given {@link Map}.
		 * @param <V> the value type of the given {@link Map}.
		 * @return new instance of {@link MapRecord}.
		 */
		static <S, K, V> MapRecord<S, K, V> of(Map<K, V> map) {

			Assert.notNull(map, "Map must not be null!");
			return StreamRecords.<S, K, V> mapBacked(map);
		}

		/**
		 * Create a new {@link ObjectRecord} instance backed by the given {@literal value}. The value may be a simple type,
		 * like {@link String} or a complex one. <br />
		 * You may want to use the builders available via {@link StreamRecords}.
		 *
		 * @param value the value to persist.
		 * @param <V> the type of the backing value.
		 * @return new instance of {@link MapRecord}.
		 */
		static <S, V> ObjectRecord<S, V> of(V value) {

			Assert.notNull(value, "Value must not be null!");
			return StreamRecords.objectBacked(value);
		}

		/**
		 * Create a new instance of {@link Record} with the given {@link RecordId}.
		 *
		 * @param id must not be {@literal null}.
		 * @return new instance of {@link Record}.
		 */
		Record<S, V> withId(RecordId id);

		/**
		 * Create a new instance of {@link Record} with the given {@literal key} to store the record at.
		 *
		 * @param key the Redis key identifying the stream.
		 * @param <S1>
		 * @return new instance of {@link Record}.
		 */
		<S1> Record<S1, V> withStreamKey(S1 key);
	}

	/**
	 * A {@link Record} within the stream mapped to a single object. This may be a simple type, such as {@link String} or
	 * a complex one.
	 *
	 * @param <V> the type of the backing Object.
	 */
	interface ObjectRecord<S, V> extends Record<S, V> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
		 */
		@Override
		ObjectRecord<S, V> withId(RecordId id);

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withStreamKey(java.lang.Object)
		 */
		<S1> ObjectRecord<S1, V> withStreamKey(S1 key);

		/**
		 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
		 * {@link RecordId id} is carried over to the new instance.
		 *
		 * @param mapper must not be {@literal null}.
		 * @param <HK> the key type of the resulting {@link MapRecord}.
		 * @param <HV> the value type of the resulting {@link MapRecord}.
		 * @return new instance of {@link MapRecord}.
		 */
		default <HK, HV> MapRecord<S, HK, HV> toMapRecord(HashMapper<? super V, HK, HV> mapper) {
			return Record.<S, HK, HV> of(mapper.toHash(getValue())).withId(getId()).withStreamKey(getStream());
		}
	}

	/**
	 * A {@link Record} within the stream backed by a collection of {@literal field/value} paris.
	 *
	 * @param <K> the field type of the backing collection.
	 * @param <V> the value type of the backing collection.
	 */
	interface MapRecord<S, K, V> extends Record<S, Map<K, V>>, Iterable<Map.Entry<K, V>> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
		 */
		@Override
		MapRecord<S, K, V> withId(RecordId id);

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withStreamKey(java.lang.Object)
		 */
		<S1> MapRecord<S1, K, V> withStreamKey(S1 key);

		/**
		 * Apply the given {@link Function mapFunction} to each and every entry in the backing collection to create a new
		 * {@link MapRecord}.
		 *
		 * @param mapFunction must not be {@literal null}.
		 * @param <HK> the field type of the new backing collection.
		 * @param <HV> the value type of the new backing collection.
		 * @return new instance of {@link MapRecord}.
		 */
		default <HK, HV> MapRecord<S, HK, HV> mapEntries(Function<Entry<K, V>, Entry<HK, HV>> mapFunction) {

			Map<HK, HV> mapped = new LinkedHashMap<>();
			iterator().forEachRemaining(it -> {

				Entry<HK, HV> mappedPair = mapFunction.apply(it);
				mapped.put(mappedPair.getKey(), mappedPair.getValue());
			});

			return StreamRecords.newRecord().in(getStream()).withId(getId()).ofMap(mapped);
		}

		default <S1, HK, HV> MapRecord<S1, HK, HV> map(Function<MapRecord<S, K, V>, MapRecord<S1, HK, HV>> mapFunction) {
			return mapFunction.apply(this);
		}

		/**
		 * Serialize {@link #getStream() key} and {@link #getValue() field/value pairs} with the given
		 * {@link RedisSerializer}. An already assigned {@link RecordId id} is carried over to the new instance.
		 *
		 * @param serializer can be {@literal null} if the {@link Record} only holds binary data.
		 * @return new {@link ByteRecord} holding the serialized values.
		 */
		default ByteRecord serialize(@Nullable RedisSerializer serializer) {
			return serialize(serializer, serializer, serializer);
		}

		/**
		 * Serialize {@link #getStream() key} with the {@literal streamSerializer}, field names with the
		 * {@literal fieldSerializer} and values with the {@literal valueSerializer}. An already assigned {@link RecordId
		 * id} is carried over to the new instance.
		 * 
		 * @param streamSerializer can be {@literal null} if the key is binary.
		 * @param fieldSerializer can be {@literal null} if the fields are binary.
		 * @param valueSerializer can be {@literal null} if the values are binary.
		 * @return new {@link ByteRecord} holding the serialized values.
		 */
		default ByteRecord serialize(@Nullable RedisSerializer<? super S> streamSerializer,
									 @Nullable RedisSerializer<? super K> fieldSerializer, @Nullable RedisSerializer<? super V> valueSerializer) {

			MapRecord<S, byte[], byte[]> x = mapEntries(it -> Collections
					.singletonMap(fieldSerializer != null ? fieldSerializer.serialize(it.getKey()) : (byte[]) it.getKey(),
							valueSerializer != null ? valueSerializer.serialize(it.getValue()) : (byte[]) it.getValue())
					.entrySet().iterator().next());

			return StreamRecords.newRecord() //
					.in(streamSerializer != null ? streamSerializer.serialize(getStream()) : (byte[]) getStream()) //
					.withId(getId()) //
					.ofBytes(x.getValue());
		}

		/**
		 * Apply the given {@link HashMapper} to the backing value to create a new {@link MapRecord}. An already assigned
		 * {@link RecordId id} is carried over to the new instance.
		 *
		 * @param mapper must not be {@literal null}.
		 * @param <OV> type of the value backing the {@link ObjectRecord}.
		 * @return new instance of {@link ObjectRecord}.
		 */
		default <OV> ObjectRecord<S, OV> toObjectRecord(HashMapper<? super OV, ? super K, ? super V> mapper) {
			return Record.<S, OV> of((OV) (mapper).fromHash((Map) getValue())).withId(getId()).withStreamKey(getStream());
		}
	}

	/**
	 * A {@link Record} within the stream backed by a collection of binary {@literal field/value} paris.
	 *
	 * @author Christoph Strobl
	 */
	interface ByteBufferRecord extends MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
		 */
		@Override
		ByteBufferRecord withId(RecordId id);

		ByteBufferRecord withStreamKey(ByteBuffer key);

		/**
		 * Deserialize {@link #getStream() key} and {@link #getValue() field/value pairs} with the given
		 * {@link RedisSerializer}. An already assigned {@link RecordId id} is carried over to the new instance.
		 *
		 * @param serializer can be {@literal null} if the {@link Record} only holds binary data.
		 * @return new {@link MapRecord} holding the deserialized values.
		 */
		default <T> MapRecord<T, T, T> deserialize(@Nullable RedisSerializer<T> serializer) {
			return deserialize(serializer, serializer, serializer);
		}

		/**
		 * Deserialize {@link #getStream() key} with the {@literal streamSerializer}, field names with the
		 * {@literal fieldSerializer} and values with the {@literal valueSerializer}. An already assigned {@link RecordId
		 * id} is carried over to the new instance.
		 *
		 * @param streamSerializer can be {@literal null} if the key suites already the target format.
		 * @param fieldSerializer can be {@literal null} if the fields suite already the target format.
		 * @param valueSerializer can be {@literal null} if the values suite already the target format.
		 * @return new {@link MapRecord} holding the deserialized values.
		 */
		default <K, HK, HV> MapRecord<K, HK, HV> deserialize(@Nullable RedisSerializer<? extends K> streamSerializer,
															 @Nullable RedisSerializer<? extends HK> fieldSerializer,
															 @Nullable RedisSerializer<? extends HV> valueSerializer) {

			return mapEntries(it -> Collections
					.<HK, HV> singletonMap(fieldSerializer != null ? fieldSerializer.deserialize(ByteUtils.getBytes(it.getKey())) : (HK) it.getKey(),
							valueSerializer != null ? valueSerializer.deserialize(ByteUtils.getBytes(it.getValue())) : (HV) it.getValue())
					.entrySet().iterator().next())
					.withStreamKey(streamSerializer != null ? streamSerializer.deserialize(ByteUtils.getBytes(getStream())) : (K) getStream());
		}

		/**
		 * Turn a binary {@link MapRecord} into a {@link ByteRecord}.
		 *
		 * @param source must not be {@literal null}.
		 * @return new instance of {@link ByteRecord}.
		 */
//		static ByteBufferRecord of(MapRecord<byte[], byte[], byte[]> source) {
//			return StreamRecords.newRecord().in(ByteBuffer.wrap(source.getStream())).withId(ByteBuffer.wrap(source.getId())).ofBytes(ByteBuffer.wrap(source.getValue()));
//		}

		/**
		 * Turn a binary {@link MapRecord} into a {@link ByteRecord}.
		 *
		 * @param source must not be {@literal null}.
		 * @return new instance of {@link ByteRecord}.
		 */
		static ByteBufferRecord of(MapRecord<ByteBuffer, ByteBuffer, ByteBuffer> source) {
			return StreamRecords.newRecord().in(source.getStream()).withId(source.getId()).ofBuffer(source.getValue());
		}

		default <OV> ObjectRecord<ByteBuffer, OV> toObjectRecord(HashMapper<? super OV, ? super ByteBuffer, ? super ByteBuffer> mapper) {

			Map<byte[], byte[]> targetMap = getValue().entrySet().stream().collect(Collectors.toMap(entry -> ByteUtils.getBytes(entry.getKey()), entry -> ByteUtils.getBytes(entry.getValue())));

			return Record.<ByteBuffer, OV> of((OV) (mapper).fromHash((Map) targetMap)).withId(getId()).withStreamKey(getStream());
		}
	}



	/**
	 * A {@link Record} within the stream backed by a collection of binary {@literal field/value} paris.
	 *
	 * @author Christoph Strobl
	 */
	interface ByteRecord extends MapRecord<byte[], byte[], byte[]> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
		 */
		@Override
		ByteRecord withId(RecordId id);

		ByteRecord withStreamKey(byte[] key);

		/**
		 * Deserialize {@link #getStream() key} and {@link #getValue() field/value pairs} with the given
		 * {@link RedisSerializer}. An already assigned {@link RecordId id} is carried over to the new instance.
		 *
		 * @param serializer can be {@literal null} if the {@link Record} only holds binary data.
		 * @return new {@link MapRecord} holding the deserialized values.
		 */
		default <T> MapRecord<T, T, T> deserialize(@Nullable RedisSerializer<T> serializer) {
			return deserialize(serializer, serializer, serializer);
		}

		/**
		 * Deserialize {@link #getStream() key} with the {@literal streamSerializer}, field names with the
		 * {@literal fieldSerializer} and values with the {@literal valueSerializer}. An already assigned {@link RecordId
		 * id} is carried over to the new instance.
		 * 
		 * @param streamSerializer can be {@literal null} if the key suites already the target format.
		 * @param fieldSerializer can be {@literal null} if the fields suite already the target format.
		 * @param valueSerializer can be {@literal null} if the values suite already the target format.
		 * @return new {@link MapRecord} holding the deserialized values.
		 */
		default <K, HK, HV> MapRecord<K, HK, HV> deserialize(@Nullable RedisSerializer<? extends K> streamSerializer,
				@Nullable RedisSerializer<? extends HK> fieldSerializer,
				@Nullable RedisSerializer<? extends HV> valueSerializer) {

			return mapEntries(it -> Collections
					.<HK, HV> singletonMap(fieldSerializer != null ? fieldSerializer.deserialize(it.getKey()) : (HK) it.getKey(),
							valueSerializer != null ? valueSerializer.deserialize(it.getValue()) : (HV) it.getValue())
					.entrySet().iterator().next())
							.withStreamKey(streamSerializer != null ? streamSerializer.deserialize(getStream()) : (K) getStream());
		}

		/**
		 * Turn a binary {@link MapRecord} into a {@link ByteRecord}.
		 *
		 * @param source must not be {@literal null}.
		 * @return new instance of {@link ByteRecord}.
		 */
		static ByteRecord of(MapRecord<byte[], byte[], byte[]> source) {
			return StreamRecords.newRecord().in(source.getStream()).withId(source.getId()).ofBytes(source.getValue());
		}
	}

	/**
	 * A {@link Record} within the stream backed by a collection of {@link String} {@literal field/value} paris.
	 *
	 * @author Christoph Strobl
	 */
	interface StringRecord extends MapRecord<String, String, String> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.RedisStreamCommands.Record#withId(org.springframework.data.redis.connection.RedisStreamCommands.RecordId)
		 */
		@Override
		StringRecord withId(RecordId id);

		StringRecord withStreamKey(String key);

		/**
		 * Turn a {@link MapRecord} of {@link String strings} into a {@link StringRecord}.
		 *
		 * @param source must not be {@literal null}.
		 * @return new instance of {@link StringRecord}.
		 */
		static StringRecord of(MapRecord<String, String, String> source) {
			return StreamRecords.newRecord().in(source.getStream()).withId(source.getId()).ofStrings(source.getValue());
		}
	}
}
