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

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.lang.Nullable;

/**
 * Redis stream specific operations bound to a certain key.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface BoundStreamOperations<K, V> {

	/**
	 * Acknowledge one or more messages as processed.
	 *
	 * @param group name of the consumer group.
	 * @param messageIds message Id's to acknowledge.
	 * @return length of acknowledged messages. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	@Nullable
	Long acknowledge(String group, String... messageIds);

	/**
	 * Append a message to the stream {@code key}.
	 *
	 * @param body message body.
	 * @return the message Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	@Nullable
	String add(Map<K, V> body);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param messageIds stream message Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	@Nullable
	Long delete(String... messageIds);

	/**
	 * Create a consumer group.
	 *
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	String createGroup(ReadOffset readOffset, String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean deleteConsumer(Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean destroyGroup(String group);

	/**
	 * Get the length of a stream.
	 *
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	@Nullable
	Long size();

	/**
	 * Read messages from a stream within a specific {@link Range}.
	 *
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	default List<StreamMessage<K, V>> range(Range<String> range) {
		return range(range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	@Nullable
	List<StreamMessage<K, V>> range(Range<String> range, Limit limit);

	/**
	 * Read messages from {@link ReadOffset}.
	 *
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	default List<StreamMessage<K, V>> read(ReadOffset readOffset) {
		return read(StreamReadOptions.empty(), readOffset);
	}

	/**
	 * Read messages starting from {@link ReadOffset}.
	 *
	 * @param readOptions read arguments.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	@Nullable
	List<StreamMessage<K, V>> read(StreamReadOptions readOptions, ReadOffset readOffset);

	/**
	 * Read messages starting from {@link ReadOffset}. using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	default List<StreamMessage<K, V>> read(Consumer consumer, ReadOffset readOffset) {
		return read(consumer, StreamReadOptions.empty(), readOffset);
	}

	/**
	 * Read messages starting from {@link ReadOffset}. using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	@Nullable
	List<StreamMessage<K, V>> read(Consumer consumer, StreamReadOptions readOptions, ReadOffset readOffset);

	/**
	 * Read messages from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	default List<StreamMessage<K, V>> reverseRange(Range<String> range) {
		return reverseRange(range, Limit.unlimited());
	}

	/**
	 * Read messages from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	@Nullable
	List<StreamMessage<K, V>> reverseRange(Range<String> range, Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	@Nullable
	Long trim(long count);
}
