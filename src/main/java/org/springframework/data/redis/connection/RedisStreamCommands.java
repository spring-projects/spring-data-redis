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
package org.springframework.data.redis.connection;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Stream-specific Redis commands.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tugdual Grall
 * @author Dengliming
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
	@Nullable
	default RecordId xAdd(MapRecord<byte[], byte[], byte[]> record) {
		return xAdd(record, XAddOptions.none());
	}

	/**
	 * Append the given {@link MapRecord record} to the stream stored at {@link Record#getStream()}. <br />
	 * If you prefer manual id assignment over server generated ones make sure to provide an id via
	 * {@link Record#withId(RecordId)}.
	 *
	 * @param record the {@link MapRecord record} to append.
	 * @param options additional options (eg. {@literal MAXLEN}). Must not be {@literal null}, use
	 *          {@link XAddOptions#none()} instead.
	 * @return the {@link RecordId id} after save. {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	@Nullable
	RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options);

	/**
	 * Additional options applicable for {@literal XADD} command.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	class XAddOptions {

		private static final XAddOptions NONE = new XAddOptions(null);

		private final @Nullable Long maxlen;

		private XAddOptions(@Nullable Long maxlen) {
			this.maxlen = maxlen;
		}

		/**
		 * @return
		 */
		public static XAddOptions none() {
			return NONE;
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return new instance of {@link XAddOptions}.
		 */
		public static XAddOptions maxlen(long maxlen) {
			return new XAddOptions(maxlen);
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getMaxlen() {
			return maxlen;
		}

		/**
		 * @return {@literal true} if {@literal MAXLEN} is set.
		 */
		public boolean hasMaxlen() {
			return maxlen != null && maxlen > 0;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			XAddOptions that = (XAddOptions) o;
			return ObjectUtils.nullSafeEquals(this.maxlen, that.maxlen);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(this.maxlen);
		}
	}

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer} without increasing the delivered
	 * count.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param options must not be {@literal null}.
	 * @return list of {@link RecordId ids} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	@Nullable
	List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options);

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param minIdleTime must not be {@literal null}.
	 * @param recordIds must not be {@literal null}.
	 * @return list of {@link ByteRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	@Nullable
	default List<ByteRecord> xClaim(byte[] key, String group, String newOwner, Duration minIdleTime,
			RecordId... recordIds) {
		return xClaim(key, group, newOwner, XClaimOptions.minIdle(minIdleTime).ids(recordIds));
	}

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param options must not be {@literal null}.
	 * @return list of {@link ByteRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	@Nullable
	List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options);

	/**
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	class XClaimOptions {

		private final List<RecordId> ids;
		private final Duration minIdleTime;
		private final @Nullable Duration idleTime;
		private final @Nullable Instant unixTime;
		private final @Nullable Long retryCount;
		private final boolean force;

		private XClaimOptions(List<RecordId> ids, Duration minIdleTime, @Nullable Duration idleTime,
				@Nullable Instant unixTime, @Nullable Long retryCount, boolean force) {

			this.ids = new ArrayList<>(ids);
			this.minIdleTime = minIdleTime;
			this.idleTime = idleTime;
			this.unixTime = unixTime;
			this.retryCount = retryCount;
			this.force = force;
		}

		/**
		 * Set the {@literal min-idle-time} to limit the command to messages that have been idle for at at least the given
		 * {@link Duration}.
		 *
		 * @param minIdleTime must not be {@literal null}.
		 * @return new instance of {@link XClaimOptions}.
		 */
		public static XClaimOptionsBuilder minIdle(Duration minIdleTime) {
			return new XClaimOptionsBuilder(minIdleTime);
		}

		/**
		 * Set the {@literal min-idle-time} to limit the command to messages that have been idle for at at least the given
		 * {@literal milliseconds}.
		 *
		 * @param millis
		 * @return new instance of {@link XClaimOptions}.
		 */
		public static XClaimOptionsBuilder minIdleMs(long millis) {
			return minIdle(Duration.ofMillis(millis));
		}

		/**
		 * Set the idle time since last delivery of a message. To specify a specific point in time use
		 * {@link #time(Instant)}.
		 *
		 * @param idleTime idle time.
		 * @return {@code this}.
		 */
		public XClaimOptions idle(Duration idleTime) {
			return new XClaimOptions(ids, minIdleTime, idleTime, unixTime, retryCount, force);
		}

		/**
		 * Sets the idle time to a specific unix time (in milliseconds). To define a relative idle time use
		 * {@link #idle(Duration)}.
		 *
		 * @param unixTime idle time.
		 * @return {@code this}.
		 */
		public XClaimOptions time(Instant unixTime) {
			return new XClaimOptions(ids, minIdleTime, idleTime, unixTime, retryCount, force);
		}

		/**
		 * Set the retry counter to the specified value.
		 *
		 * @param retryCount can be {@literal null}. If {@literal null} no change to the retry counter will be made.
		 * @return new instance of {@link XClaimOptions}.
		 */
		public XClaimOptions retryCount(long retryCount) {
			return new XClaimOptions(ids, minIdleTime, idleTime, unixTime, retryCount, force);
		}

		/**
		 * Forces creation of a pending message entry in the PEL even if it does not already exist as long a the given
		 * stream record id is valid.
		 *
		 * @return new instance of {@link XClaimOptions}.
		 */
		public XClaimOptions force() {
			return new XClaimOptions(ids, minIdleTime, idleTime, unixTime, retryCount, true);
		}

		/**
		 * Get the {@link List} of {@literal ID}.
		 *
		 * @return never {@literal null}.
		 */
		public List<RecordId> getIds() {
			return ids;
		}

		/**
		 * Get the {@literal ID} array as {@link String strings}.
		 *
		 * @return never {@literal null}.
		 */
		public String[] getIdsAsStringArray() {
			return getIds().stream().map(RecordId::getValue).toArray(String[]::new);
		}

		/**
		 * Get the {@literal min-idle-time}.
		 *
		 * @return never {@literal null}.
		 */
		public Duration getMinIdleTime() {
			return minIdleTime;
		}

		/**
		 * Get the {@literal IDLE ms} time.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Duration getIdleTime() {
			return idleTime;
		}

		/**
		 * Get the {@literal TIME ms-unix-time}
		 *
		 * @return
		 */
		@Nullable
		public Instant getUnixTime() {
			return unixTime;
		}

		/**
		 * Get the {@literal RETRYCOUNT count}.
		 *
		 * @return
		 */
		@Nullable
		public Long getRetryCount() {
			return retryCount;
		}

		/**
		 * Get the {@literal FORCE} flag.
		 *
		 * @return
		 */
		public boolean isForce() {
			return force;
		}

		public static class XClaimOptionsBuilder {

			private final Duration minIdleTime;

			XClaimOptionsBuilder(Duration minIdleTime) {

				Assert.notNull(minIdleTime, "Min idle time must not be null!");

				this.minIdleTime = minIdleTime;
			}

			/**
			 * Set the {@literal ID}s to claim.
			 *
			 * @param ids must not be {@literal null}.
			 * @return
			 */
			public XClaimOptions ids(List<?> ids) {

				List<RecordId> idList = ids.stream()
						.map(it -> it instanceof RecordId ? (RecordId) it : RecordId.of(it.toString()))
						.collect(Collectors.toList());

				return new XClaimOptions(idList, minIdleTime, null, null, null, false);
			}

			/**
			 * Set the {@literal ID}s to claim.
			 *
			 * @param ids must not be {@literal null}.
			 * @return
			 */
			public XClaimOptions ids(RecordId... ids) {
				return ids(Arrays.asList(ids));
			}

			/**
			 * Set the {@literal ID}s to claim.
			 *
			 * @param ids must not be {@literal null}.
			 * @return
			 */
			public XClaimOptions ids(String... ids) {
				return ids(Arrays.asList(ids));
			}
		}
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
	@Nullable
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
	 * Create a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group to create.
	 * @param readOffset the offset to start at.
	 * @param mkStream if true the group will create the stream if not already present (MKSTREAM)
	 * @return {@literal ok} if successful. {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	@Nullable
	String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream);

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
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	@Nullable
	XInfoStream xInfo(byte[] key);

	/**
	 * Obtain information about {@literal consumer groups} associated with the stream stored at the specified
	 * {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	@Nullable
	XInfoGroups xInfoGroups(byte[] key);

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the {@literal consumer group}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	@Nullable
	XInfoConsumers xInfoConsumers(byte[] key, String groupName);

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
	 * Obtain the {@link PendingMessagesSummary} for a given {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @return a summary of pending messages within the given {@literal consumer group} or {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	PendingMessagesSummary xPending(byte[] key, String groupName);

	/**
	 * Obtained detailed information about all pending messages for a given {@link Consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default PendingMessages xPending(byte[] key, Consumer consumer) {
		return xPending(key, consumer.getGroup(), consumer.getName());
	}

	/**
	 * Obtained detailed information about all pending messages for a given {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default PendingMessages xPending(byte[] key, String groupName, String consumerName) {
		return xPending(key, groupName, XPendingOptions.unbounded().consumer(consumerName));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} within a
	 * {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param idleMilliSeconds the pending messages which were idle for certain duration . Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default PendingMessages xPending(byte[] key, String groupName, Range<?> range, Long count, Long idleMilliSeconds) {
		return xPending(key, groupName, XPendingOptions.range(range, count, idleMilliSeconds));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param idleMilliSeconds the pending messages which were idle for certain duration . Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default PendingMessages xPending(byte[] key, Consumer consumer, Range<?> range, Long count, Long idleMilliSeconds) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count, idleMilliSeconds);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@literal consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the name of the {@literal consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer} in given {@literal consumer group} or {@literal null}
	 *         when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default PendingMessages xPending(byte[] key, String groupName, String consumerName, Range<?> range, Long count,
			Long idleMilliSeconds) {
		return xPending(key, groupName, XPendingOptions.range(range, count, idleMilliSeconds).consumer(consumerName));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} applying given {@link XPendingOptions
	 * options}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param options the options containing {@literal range}, {@literal consumer} and {@literal count}. Must not be
	 *          {@literal null}.
	 * @return pending messages matching given criteria or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	PendingMessages xPending(byte[] key, String groupName, XPendingOptions options);

	/**
	 * Value Object holding parameters for obtaining pending messages.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	class XPendingOptions {

		private final @Nullable String consumerName;
		private final Range<?> range;
		private final @Nullable Long count;
		private final @Nullable Long idleMilliseconds;

		private XPendingOptions(@Nullable String consumerName, Range<?> range, @Nullable Long count,
				@Nullable Long idleMilliseconds) {

			this.range = range;
			this.count = count;
			this.consumerName = consumerName;
			this.idleMilliseconds = idleMilliseconds;
		}

		/**
		 * Create new {@link XPendingOptions} with an unbounded {@link Range} ({@literal - +}).
		 *
		 * @return new instance of {@link XPendingOptions}.
		 */
		public static XPendingOptions unbounded() {
			return new XPendingOptions(null, Range.unbounded(), null, null);
		}

		/**
		 * Create new {@link XPendingOptions} with an unbounded {@link Range} ({@literal - +}).
		 *
		 * @param count the max number of messages to return. Must not be {@literal null}.
		 * @return new instance of {@link XPendingOptions}.
		 */
		public static XPendingOptions unbounded(Long count) {
			return new XPendingOptions(null, Range.unbounded(), count, null);
		}

		/**
		 * StreamOperations Create new {@link XPendingOptions} with given {@link Range}, limit and messages which are idle
		 * for certain time.
		 *
		 * @return new instance of {@link XPendingOptions}.
		 */
		public static XPendingOptions range(Range<?> range, Long count, Long idleMilliseconds) {
			return new XPendingOptions(null, range, count, idleMilliseconds);
		}

		/**
		 * Append given consumer.
		 *
		 * @param consumerName must not be {@literal null}.
		 * @return new instance of {@link XPendingOptions}.
		 */
		public XPendingOptions consumer(String consumerName) {
			return new XPendingOptions(consumerName, range, count, idleMilliseconds);
		}

		/**
		 * @return never {@literal null}.
		 */
		public Range<?> getRange() {
			return range;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getCount() {
			return count;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public String getConsumerName() {
			return consumerName;
		}

		/**
		 * @return {@literal true} if a consumer name is present.
		 */
		public boolean hasConsumer() {
			return StringUtils.hasText(consumerName);
		}

		/**
		 * @return {@literal true} count is set.
		 */
		public boolean isLimited() {
			return count != null && count > -1;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getIdleMilliseconds() {
			return idleMilliseconds;
		}
	}

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
	Long xTrim(byte[] key, long count, boolean approximateTrimming);
}
