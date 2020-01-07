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
package org.springframework.data.redis.connection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.lang.Nullable;

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
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
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
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long xAck(byte[] key, String group, RecordId... recordIds);

	/**
	 * Append a new record with the given {@link Map field/value pairs} as content to the stream stored at {@code key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param content the records content modeled as {@link Map field/value pairs}.
	 * @return the server generated {@link RecordId id}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
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
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
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
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
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
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long xLen(byte[] key);

	/**
	 * Retrieve all {@link ByteRecord records} within a specific {@link Range} from the stream stored at {@literal key}.
	 * <br />
	 * Use {@link Range#unbounded()} to read from the minimum and the maximum ID possible.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
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
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
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
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
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
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
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
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long xTrim(byte[] key, long count);
}
