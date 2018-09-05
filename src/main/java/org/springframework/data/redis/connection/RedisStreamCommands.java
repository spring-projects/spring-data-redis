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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stream-specific Redis commands.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface RedisStreamCommands {

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param messageIds message Id's to acknowledge.
	 * @return length of acknowledged messages. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long xAck(byte[] key, String group, String... messageIds);

	/**
	 * Append a message to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param body message body.
	 * @return the message Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	String xAdd(byte[] key, Map<byte[], byte[]> body);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param messageIds stream message Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	Long xDel(byte[] key, String... messageIds);

	/**
	 * Create a consumer group.
	 *
	 * @param key the stream key.
	 * @param readOffset the offset to start with.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String xGroupCreate(byte[] key, ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDelConsumer(byte[] key, Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean xGroupDestroy(byte[] key, String group);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long xLen(byte[] key);

	/**
	 * Read messages from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xRange(byte[] key, Range<String> range) {
		return xRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<StreamMessage<byte[], byte[]>> xRange(byte[] key, Range<String> range, Limit limit);

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xRead(StreamOffset<byte[]> stream) {
		return xRead(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xRead(StreamOffset<byte[]>... streams) {
		return xRead(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xRead(StreamReadOptions readOptions, StreamOffset<byte[]> stream) {
		return xRead(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<StreamMessage<byte[], byte[]>> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams);

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xReadGroup(Consumer consumer, StreamOffset<byte[]> stream) {
		return xReadGroup(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xReadGroup(Consumer consumer, StreamOffset<byte[]>... streams) {
		return xReadGroup(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]> stream) {
		return xReadGroup(consumer, readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read messages from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<StreamMessage<byte[], byte[]>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams);

	/**
	 * Read messages from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<StreamMessage<byte[], byte[]>> xRevRange(byte[] key, Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<StreamMessage<byte[], byte[]>> xRevRange(byte[] key, Range<String> range, Limit limit);

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
	 * A stream message and its id.
	 *
	 * @author Mark Paluch
	 */
	@EqualsAndHashCode
	@Getter
	class StreamMessage<K, V> {

		private final K stream;
		private final String id;
		private final Map<K, V> body;

		/**
		 * Create a new {@link io.lettuce.core.StreamMessage}.
		 *
		 * @param stream the stream.
		 * @param id the message id.
		 * @param body map containing the message body.
		 */
		public StreamMessage(K stream, String id, Map<K, V> body) {

			this.stream = stream;
			this.id = id;
			this.body = body;
		}

		@Override
		public String toString() {
			return String.format("StreamMessage[%s:%s]%s", stream, id, body);
		}
	}

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
}
