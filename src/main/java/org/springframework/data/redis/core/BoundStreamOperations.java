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

import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XDelOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamEntryDeletionResult;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamReadOptions;

/**
 * Redis stream specific operations bound to a certain key.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Dengliming
 * @author Viktoriya Kutsarova
 * @since 2.2
 */
@NullUnmarked
public interface BoundStreamOperations<K, HK, HV> {

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param group name of the consumer group.
	 * @param recordIds record Id's to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	Long acknowledge(@NonNull String group, @NonNull String @NonNull... recordIds);

	/**
	 * Append a record to the stream {@code key}.
	 *
	 * @param body record body.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	RecordId add(@NonNull Map<@NonNull HK, HV> body);

	/**
	 * Append a record to the stream {@code key} with the specified options.
	 *
	 * @param content record content as Map.
	 * @param xAddOptions additional parameters for the {@literal XADD} call.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 * @since 3.4
	 */
	RecordId add(@NonNull Map<@NonNull HK, HV> content, @NonNull XAddOptions xAddOptions);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param recordIds stream record Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	Long delete(@NonNull String @NonNull... recordIds);

	/**
	 * Deletes one or multiple entries from the stream at the specified key with extended options.
	 *
	 * @param options the {@link XDelOptions} specifying deletion policy.
	 * @param recordIds stream record Id's as strings.
	 * @return list of {@link StreamEntryDeletionResult} for each ID.
	 * @see <a href="https://redis.io/commands/xdelex">Redis Documentation: XDELEX</a>
	 * @since 4.1
	 */
	List<StreamEntryDeletionResult> deleteWithOptions(@NonNull XDelOptions options, @NonNull String @NonNull ... recordIds);

	/**
	 * Deletes one or multiple entries from the stream at the specified key with extended options.
	 *
	 * @param options the {@link XDelOptions} specifying deletion policy.
	 * @param recordIds stream record Id's.
	 * @return list of {@link StreamEntryDeletionResult} for each ID.
	 * @see <a href="https://redis.io/commands/xdelex">Redis Documentation: XDELEX</a>
	 * @since 4.1
	 */
	List<StreamEntryDeletionResult> deleteWithOptions(@NonNull XDelOptions options, @NonNull RecordId @NonNull ... recordIds);

	/**
	 * Acknowledges and conditionally deletes one or multiple entries for a stream consumer group at the specified key.
	 *
	 * @param group name of the consumer group.
	 * @param options the {@link XDelOptions} specifying deletion policy.
	 * @param recordIds stream record Id's as strings.
	 * @return list of {@link StreamEntryDeletionResult} for each ID.
	 * @see <a href="https://redis.io/commands/xackdel">Redis Documentation: XACKDEL</a>
	 * @since 4.1
	 */
	List<StreamEntryDeletionResult> acknowledgeAndDelete(@NonNull String group, @NonNull XDelOptions options,
														 @NonNull String @NonNull ... recordIds);

	/**
	 * Acknowledges and conditionally deletes one or multiple entries for a stream consumer group at the specified key.
	 *
	 * @param group name of the consumer group.
	 * @param options the {@link XDelOptions} specifying deletion policy.
	 * @param recordIds stream record Id's.
	 * @return list of {@link StreamEntryDeletionResult} for each ID.
	 * @see <a href="https://redis.io/commands/xackdel">Redis Documentation: XACKDEL</a>
	 * @since 4.1
	 */
	List<StreamEntryDeletionResult> acknowledgeAndDelete(@NonNull String group, @NonNull XDelOptions options,
														 @NonNull RecordId @NonNull ... recordIds);

	/**
	 * Create a consumer group.
	 *
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	String createGroup(@NonNull ReadOffset readOffset, @NonNull String group);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param consumer consumer identified by group name and consumer key.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Boolean deleteConsumer(@NonNull Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Boolean destroyGroup(@NonNull String group);

	/**
	 * Get the length of a stream.
	 *
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Long size();

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default List<@NonNull MapRecord<K, HK, HV>> range(@NonNull Range<String> range) {
		return range(range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	List<@NonNull MapRecord<K, HK, HV>> range(@NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Read records from {@link ReadOffset}.
	 *
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default List<@NonNull MapRecord<K, HK, HV>> read(@NonNull ReadOffset readOffset) {
		return read(StreamReadOptions.empty(), readOffset);
	}

	/**
	 * Read records starting from {@link ReadOffset}.
	 *
	 * @param readOptions read arguments.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	List<@NonNull MapRecord<K, HK, HV>> read(@NonNull StreamReadOptions readOptions, @NonNull ReadOffset readOffset);

	/**
	 * Read records starting from {@link ReadOffset}. using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default List<@NonNull MapRecord<K, HK, HV>> read(@NonNull Consumer consumer, @NonNull ReadOffset readOffset) {
		return read(consumer, StreamReadOptions.empty(), readOffset);
	}

	/**
	 * Read records starting from {@link ReadOffset}. using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param readOffset the offset to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	List<@NonNull MapRecord<K, HK, HV>> read(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			@NonNull ReadOffset readOffset);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default List<@NonNull MapRecord<K, HK, HV>> reverseRange(@NonNull Range<String> range) {
		return reverseRange(range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	List<@NonNull MapRecord<K, HK, HV>> reverseRange(@NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long trim(long count);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param count length of the stream.
	 * @param approximateTrimming the trimming must be performed in a approximated way in order to maximize performances.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long trim(long count, boolean approximateTrimming);

	/**
	 * Trims the stream according to the specified {@link RedisStreamCommands.XTrimOptions}.
	 * <p>
	 * Supports various trimming strategies including {@literal MAXLEN} (limit by count) and
	 * {@literal MINID} (evict entries older than a specific ID), with options for approximate
	 * or exact trimming.
	 *
	 * @param options the trimming options specifying the strategy and parameters. Must not be {@literal null}.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.7.4
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long trim(@NonNull XTrimOptions options);
}
