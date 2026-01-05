/*
 * Copyright 2018-present the original author or authors.
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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
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
 * @author Mark John Moreno
 * @author Jeonggyu Choi
 * @author Viktoriya Kutsarova
 * @since 2.2
 * @see RedisCommands
 * @see <a href="https://redis.io/topics/streams-intro">Redis Documentation - Streams</a>
 */
@NullUnmarked
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
	default Long xAck(byte @NonNull [] key, @NonNull String group, @NonNull String @NonNull... recordIds) {
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
	Long xAck(byte @NonNull [] key, @NonNull String group, @NonNull RecordId @NonNull... recordIds);

	/**
	 * Append a new record with the given {@link Map field/value pairs} as content to the stream stored at {@code key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param content the records content modeled as {@link Map field/value pairs}.
	 * @return the server generated {@link RecordId id}. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default RecordId xAdd(byte @NonNull [] key, @NonNull Map<byte @NonNull [], byte @NonNull []> content) {
		return xAdd(StreamRecords.newRecord().in(key).ofMap(content));
	}

	/**
	 * Append the given {@link MapRecord record} to the stream stored at {@code Record#getStream}. If you prefer manual id
	 * assignment over server generated ones make sure to provide an id via {@code Record#withId}.
	 *
	 * @param record the {@link MapRecord record} to append.
	 * @return the {@link RecordId id} after save. {@literal null} when used in pipeline / transaction.
	 */
	default RecordId xAdd(@NonNull MapRecord<byte[], byte[], byte[]> record) {
		return xAdd(record, XAddOptions.none());
	}

	/**
	 * Append the given {@link MapRecord record} to the stream stored at {@code Record#getStream}. If you prefer manual id
	 * assignment over server generated ones make sure to provide an id via {@code Record#withId}.
	 *
	 * @param record the {@link MapRecord record} to append.
	 * @param options additional options (e.g. {@literal MAXLEN}). Must not be {@literal null}, use
	 *          {@link XAddOptions#none()} instead.
	 * @return the {@link RecordId id} after save. {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, @NonNull XAddOptions options);

	/**
	 * The trimming strategy.
	 * @since 4.1
	 */
	sealed interface TrimStrategy permits MaxLenTrimStrategy, MinIdTrimStrategy {
	}

	/**
	 * Trimming strategy that evicts entries as long as the stream's length exceeds the specified threshold.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: MAXLEN option for XTRIM</a>
	 * @since 4.1
	 */
	final class MaxLenTrimStrategy implements TrimStrategy {
		private final long threshold;

		private MaxLenTrimStrategy(long threshold) {
			this.threshold = threshold;
		}

		/**
		 * The maximum number of entries allowed in the stream.
		 *
		 * @return non-negative number of entries allowed in the stream
		 */
		public long threshold() {
			return threshold;
		}

	}

	/**
	 * Trimming strategy that evicts entries with IDs lower than the specified threshold.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: MINID option for XTRIM</a>
	 * @since 4.1
	 */
	final class MinIdTrimStrategy implements TrimStrategy {
		private final RecordId threshold;

		private MinIdTrimStrategy(RecordId threshold) {
			this.threshold = threshold;
		}

		/**
		 * The lowest stream ID allowed in the stream - all entries whose IDs are less than threshold are trimmed.
		 *
		 * @return the lowest stream ID allowed in the stream
		 */
		public RecordId threshold() {
			return threshold;
		}
	}

	enum TrimOperator {
		EXACT,
		APPROXIMATE
	}

	@NullMarked
	class TrimOptions {

		private final TrimStrategy trimStrategy;
		private final TrimOperator trimOperator;
		private final @Nullable Long limit;
		private final @Nullable StreamDeletionPolicy deletionPolicy;

		private TrimOptions(TrimStrategy trimStrategy, TrimOperator trimOperator, @Nullable Long limit, @Nullable StreamDeletionPolicy deletionPolicy) {
			this.trimStrategy = trimStrategy;
			this.trimOperator = trimOperator;
			this.limit = limit;
			this.deletionPolicy = deletionPolicy;
		}


		/**
		 * Create trim options using the MAXLEN strategy with the given threshold.
		 * <p>
		 * Produces {@link TrimOptions} with the exact ("=") operator by default; call {@link #approximate()} to use
		 * approximate ("~") trimming.
		 *
		 * @param maxLen maximum number of entries to retain in the stream
		 * @return new {@link TrimOptions} configured with the MAXLEN strategy
		 * @since 4.0
		 */
		public static TrimOptions maxLen(long maxLen) {
			return new TrimOptions(new MaxLenTrimStrategy(maxLen), TrimOperator.EXACT, null, null);
		}


		/**
		 * Create trim options using the MINID strategy with the given minimum id.
		 * <p>
		 * Produces {@link TrimOptions} with the exact ("=") operator by default; call {@link #approximate()} to use
		 * approximate ("~") trimming.
		 *
		 * @param minId minimum id; entries with an id lower than this value are eligible for trimming
		 * @return new {@link TrimOptions} configured with the MINID strategy
		 * @since 4.0
		 */
		public static TrimOptions minId(RecordId minId) {
			return new TrimOptions(new MinIdTrimStrategy(minId), TrimOperator.EXACT, null, null);
		}

		/**
		 * Apply specified trim operator.
		 * <p>
		 * This is a member method that preserves all other options.
		 *
		 * @param trimOperator the operator to use when trimming
		 * @return new instance of {@link XTrimOptions}.
		 */
		public TrimOptions trim(TrimOperator trimOperator) {
			return new TrimOptions(trimStrategy, trimOperator, limit, deletionPolicy);
		}

		/**
		 * Use approximate trimming ("~").
		 * <p>
		 * This is a member method that preserves all other options.
		 *
		 * @return new instance of {@link TrimOptions} with {@link TrimOperator#APPROXIMATE}.
		 */
		public TrimOptions approximate() {
			return new TrimOptions(trimStrategy, TrimOperator.APPROXIMATE, limit, deletionPolicy);
		}

		/**
		 * Use exact trimming ("=").
		 * <p>
		 * This is a member method that preserves all other options.
		 *
		 * @return new instance of {@link TrimOptions} with {@link TrimOperator#EXACT}.
		 */
		public TrimOptions exact() {
			return new TrimOptions(trimStrategy, TrimOperator.EXACT, limit, deletionPolicy);
		}


		/**
		 * Limit the maximum number of entries considered when trimming.
		 * <p>
		 * This is a member method that preserves all other options.
		 *
		 * @param limit the maximum number of entries to examine for trimming.
		 * @return new instance of {@link XTrimOptions}.
		 */
		public TrimOptions limit(long limit) {
			return new TrimOptions(trimStrategy, trimOperator, limit, deletionPolicy);
		}

		/**
		 * Set the deletion policy for trimming.
		 * <p>
		 * This is a member method that preserves all other options.
		 *
		 * @param deletionPolicy the deletion policy to apply.
		 * @return new instance of {@link XTrimOptions}.
		 */
		public TrimOptions deletionPolicy(StreamDeletionPolicy deletionPolicy) {
			return new TrimOptions(trimStrategy, trimOperator, limit, deletionPolicy);
		}

		public TrimStrategy getTrimStrategy() {
			return trimStrategy;
		}

		/**
		 * @return strategy to use when trimming entries
		 */
		public TrimOperator getTrimOperator() {
			return trimOperator;
		}

		/**
		 * @return the limit to retain during trimming.
		 * @since 4.0
		 */
		public @Nullable Long getLimit() {
			return limit;
		}

		/**
		 * @return {@literal true} if {@literal LIMIT} is set.
		 * @since 4.0
		 */
		public boolean hasLimit() {
			return limit != null;
		}

		/**
		 * @return the deletion policy.
		 * @since 4.0
		 */
		public @Nullable StreamDeletionPolicy getDeletionPolicy() {
			return deletionPolicy;
		}

		/**
		 * @return {@literal true} if {@literal DELETION_POLICY} is set.
		 * @since 4.0
		 */
		public boolean hasDeletionPolicy() {
			return deletionPolicy != null;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof TrimOptions that)) {
				return false;
			}
			if (this.trimStrategy.equals(that.trimStrategy)) {
				return false;
			}
			if (this.trimOperator.equals(that.trimOperator)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(deletionPolicy, that.deletionPolicy);
		}

		@Override
		public int hashCode() {
			int result = trimStrategy.hashCode();
			result = 31 * result + trimOperator.hashCode();
			result = 31 * result + ObjectUtils.nullSafeHashCode(limit);
			result = 31 * result + ObjectUtils.nullSafeHashCode(deletionPolicy);
			return result;
		}
	}

	@NullMarked
	class XTrimOptions {

		private final TrimOptions trimOptions;

		private XTrimOptions(TrimOptions trimOptions) {
			this.trimOptions = trimOptions;
		}

		public static XTrimOptions trim(TrimOptions trimOptions) {
			return new XTrimOptions(trimOptions);
		}

		/**
		 * Backward-compatible factory alias for creating {@link XTrimOptions} from {@link TrimOptions}.
		 *
		 * @param trimOptions the trim options to apply for XTRIM
		 * @return new {@link XTrimOptions}
		 * @since 4.1
		 */
		public static XTrimOptions of(TrimOptions trimOptions) {
			return trim(trimOptions);
		}


		public TrimOptions getTrimOptions() {
			return trimOptions;
		}
	}

	/**
	 * Additional options applicable for {@literal XADD} command.
	 *
	 * @author Christoph Strobl
	 * @author Mark John Moreno
	 * @author Liming Deng
	 * @since 2.3
	 */
	@NullMarked
	class XAddOptions {

		public static XAddOptions NONE = new XAddOptions(false, null);

		private final boolean nomkstream;
		private final @Nullable TrimOptions trimOptions;

		private XAddOptions(boolean nomkstream, @Nullable TrimOptions trimOptions) {
			this.nomkstream = nomkstream;
			this.trimOptions = trimOptions;
		}

		/**
		 * Create default add options.
		 *
		 * @return new instance of {@link XAddOptions} with default values
		 * @since 2.6
		 */
		public static XAddOptions none() {
			return NONE;
		}

		public static XAddOptions trim(@Nullable TrimOptions trimOptions) {
			return new XAddOptions(false, trimOptions);
		}

		/**
		 * Disable creation of stream if it does not already exist.
		 *
		 * @return new instance of {@link XAddOptions}.
		 * @since 2.6
		 */
		public static XAddOptions makeNoStream() {
			return new XAddOptions(true, null);
		}

		/**
		 * Whether to disable creation of stream if it does not already exist.
		 *
		 * @param makeNoStream {@code true} to not create a stream if it does not already exist.
		 * @return new instance of {@link XAddOptions}.
		 * @since 2.6
		 */
		public static XAddOptions makeNoStream(boolean makeNoStream) {
			return new XAddOptions(makeNoStream, null);
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return new instance of {@link XAddOptions}.
		 */
		public static XAddOptions maxlen(long maxlen) {
			return new XAddOptions(false, TrimOptions.maxLen(maxlen));
		}

		/**
		 * Apply {@code MINID} trimming strategy, that evicts entries with IDs lower than the one specified.
		 *
		 * @param minId the minimum record Id to retain.
		 * @return new instance of {@link XAddOptions}.
		 * @since 2.7
		 */
		public XAddOptions minId(RecordId minId) {
			return new XAddOptions(nomkstream, TrimOptions.minId(minId));
		}

		/**
		 * Apply efficient trimming for capped streams using the {@code ~} flag.
		 *
		 * @return new instance of {@link XAddOptions}.
		 * @deprecated since 4.0: callers must specify a concrete trim strategy (MAXLEN or MINID)
		 * via {@link TrimOptions}; do not use this method to only toggle approximate/exact.
		 * Prefer {@code XAddOptions.trim(TrimOptions.maxLen(n).approximate())} or
		 * {@code XAddOptions.trim(TrimOptions.minId(id).exact())}.
		 */
		@Deprecated(since = "4.0", forRemoval = false)
		public XAddOptions approximateTrimming(boolean approximateTrimming) {
			TrimOptions trimOptions = this.trimOptions != null ? this.trimOptions : TrimOptions.maxLen(0);
			if (approximateTrimming) {
				return new XAddOptions(nomkstream, trimOptions.approximate());
			} else {
				return new XAddOptions(nomkstream, trimOptions.exact());
			}
		}

		/**
		 * @return {@literal true} if {@literal NOMKSTREAM} is set.
		 * @since 2.6
		 */
		public boolean isNoMkStream() {
			return nomkstream;
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return can be {@literal null}.
		 */
		public @Nullable Long getMaxlen() {
			return trimOptions != null && trimOptions.getTrimStrategy() instanceof MaxLenTrimStrategy maxLenTrimStrategy
					? maxLenTrimStrategy.threshold() : null;
		}

		/**
		 * @return {@literal true} if {@literal MAXLEN} is set.
		 */
		public boolean hasMaxlen() {
			return trimOptions != null && trimOptions.getTrimStrategy() instanceof MaxLenTrimStrategy;
		}

		/**
		 * @return {@literal true} if {@literal approximateTrimming} is set.
		 */
		public boolean isApproximateTrimming() {
			return trimOptions != null && trimOptions.getTrimOperator() == TrimOperator.APPROXIMATE;
		}

		/**
		 * @return the minimum record id to retain during trimming.
		 * @since 2.7
		 */
		public @Nullable RecordId getMinId() {
			return trimOptions != null && trimOptions.getTrimStrategy() instanceof MinIdTrimStrategy minIdTrimStrategy
					? minIdTrimStrategy.threshold() : null;
		}

		/**
		 * @return {@literal true} if {@literal MINID} is set.
		 * @since 2.7
		 */
		public boolean hasMinId() {
			return trimOptions != null && trimOptions.getTrimStrategy() instanceof MinIdTrimStrategy;
		}

		public XAddOptions nomkstream(boolean nomkstream) {
			return new XAddOptions(nomkstream, trimOptions);
		}

		public boolean hasTrimOptions() {
			return trimOptions != null;
		}

		public @Nullable TrimOptions getTrimOptions() {
			return trimOptions;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof XAddOptions that)) {
				return false;
			}
			if (!(ObjectUtils.nullSafeEquals(this.trimOptions, that.trimOptions))) {
				return false;
			}
			return nomkstream == that.nomkstream;
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(this.trimOptions);
			result = 31 * result + (nomkstream ? 1 : 0);
			return result;
		}
	}

	/**
	 * Deletion policy for stream entries - specifies how to handle consumer group references when deleting stream
	 * entries.
	 *
	 * @author Viktoriya Kutsarova
	 * @since 4.1
	 */
	enum StreamDeletionPolicy {

		/**
		 * Remove entries according to the specified strategy, but preserve existing references to these entries in all
		 * consumer groups' PEL (Pending Entries List).
		 */
		KEEP_REFERENCES,

		/**
		 * Remove entries according to the specified strategy and remove all references to these entries from all
		 * consumer groups' PEL.
		 */
		DELETE_REFERENCES,

		/**
		 * Remove entries that meet the specified strategy and that have been read and acknowledged by all
		 * consumer groups.
		 */
		ACKNOWLEDGED;

		/**
		 * Factory method for {@link #KEEP_REFERENCES}.
		 */
		public static StreamDeletionPolicy keep() { return KEEP_REFERENCES; }

		/**
		 * Factory method for {@link #DELETE_REFERENCES}.
		 */
		public static StreamDeletionPolicy delete() { return DELETE_REFERENCES; }

		/**
		 * Factory method for {@link #ACKNOWLEDGED}.
		 */
		public static StreamDeletionPolicy removeAcknowledged() { return ACKNOWLEDGED; }
	}

	/**
	 * Result of a stream entry deletion operation for {@literal XDELEX} and {@literal XACKDEL} commands.
	 *
	 * @author Viktoriya Kutsarova
	 * @since 4.1
	 */
	enum StreamEntryDeletionResult {

		UNKNOWN(-2L),

		/**
		 * The entry ID does not exist in the stream.
		 */
		NOT_FOUND(-1L),

		/**
		 * The entry was successfully deleted from the stream.
		 */
		DELETED(1L),

		/**
		 * The entry was acknowledged but not deleted (when using ACKED deletion policy with dangling references).
		 */
		NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED(2L);

		private final long code;

		StreamEntryDeletionResult(long code) {
			this.code = code;
		}

		/**
		 * Get the numeric code for this deletion result.
		 *
		 * @return the numeric code: -1 for NOT_FOUND, 1 for DELETED, 2 for NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED
		 */
		public long getCode() {
			return code;
		}

		/**
		 * Convert a numeric code to a {@link StreamEntryDeletionResult}.
		 *
		 * @param code the numeric code
		 * @return the corresponding {@link StreamEntryDeletionResult}
		 * @throws IllegalArgumentException if the code is not valid
		 */
		public static StreamEntryDeletionResult fromCode(long code) {
			return switch ((int) code) {
				case -2 -> UNKNOWN;
				case -1 -> NOT_FOUND;
				case 1 -> DELETED;
				case 2 -> NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED;
				default -> throw new IllegalArgumentException("Invalid deletion result code: " + code);
			};
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
	List<@NonNull RecordId> xClaimJustId(byte @NonNull [] key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options);

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
	default List<@NonNull ByteRecord> xClaim(byte @NonNull [] key, @NonNull String group, @NonNull String newOwner,
			@NonNull Duration minIdleTime, @NonNull RecordId @NonNull... recordIds) {
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
	List<@NonNull ByteRecord> xClaim(byte @NonNull [] key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options);

	/**
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	@NullMarked
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
		public @Nullable Duration getIdleTime() {
			return idleTime;
		}

		/**
		 * Get the {@literal TIME ms-unix-time}
		 *
		 * @return
		 */
		public @Nullable Instant getUnixTime() {
			return unixTime;
		}

		/**
		 * Get the {@literal RETRYCOUNT count}.
		 *
		 * @return
		 */
		public @Nullable Long getRetryCount() {
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

		@NullMarked
		public static class XClaimOptionsBuilder {

			private final Duration minIdleTime;

			XClaimOptionsBuilder(Duration minIdleTime) {

				Assert.notNull(minIdleTime, "Min idle time must not be null");

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
	default Long xDel(byte @NonNull [] key, @NonNull String @NonNull... recordIds) {
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
	Long xDel(byte @NonNull [] key, @NonNull RecordId @NonNull... recordIds);

	/**
	 * Additional options applicable for {@literal XDELEX} and {@literal XACKDEL} commands.
	 *
	 * @author Viktoriya Kutsarova
	 * @since 4.1
	 */
	@NullMarked
	class XDelOptions {

		private static final XDelOptions DEFAULT = new XDelOptions(StreamDeletionPolicy.keep());

		private final StreamDeletionPolicy deletionPolicy;

		private XDelOptions(StreamDeletionPolicy deletionPolicy) {
			this.deletionPolicy = deletionPolicy;
		}

		/**
		 * Create an {@link XDelOptions} instance with default options.
		 * <p>
		 * This returns the default options for the {@literal XDELEX} and {@literal XACKDEL} commands
		 * with {@link StreamDeletionPolicy#KEEP_REFERENCES} as the deletion policy, which preserves
		 * existing references in consumer groups' PELs (similar to the behavior of {@literal XDEL}).
		 *
		 * @return a default {@link XDelOptions} instance with {@link StreamDeletionPolicy#KEEP_REFERENCES}.
		 */
		public static XDelOptions defaults() {
			return DEFAULT;
		}

		/**
		 * Set the deletion policy for the delete operation.
		 *
		 * @param deletionPolicy the deletion policy to apply.
		 * @return new instance of {@link XDelOptions}.
		 */
		public static XDelOptions deletionPolicy(StreamDeletionPolicy deletionPolicy) {
			return new XDelOptions(deletionPolicy);
		}

		/**
		 * @return the deletion policy.
		 */
		public StreamDeletionPolicy getDeletionPolicy() {
			return deletionPolicy;
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof XDelOptions that)) {
				return false;
			}
			return deletionPolicy.equals(that.deletionPolicy);
		}

		@Override
		public int hashCode() {
			return deletionPolicy.hashCode();
		}
	}

	/**
	 * Deletes one or multiple entries from the stream at the specified key.
	 * <p>
	 * XDELEX is an extension of the Redis Streams XDEL command that provides more control over how message entries
	 * are deleted concerning consumer groups.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param options the {@link XDelOptions} specifying deletion policy. Use {@link XDelOptions#defaults()} ()} for default behavior.
	 * @param recordIds the id's of the records to remove.
	 * @return list of {@link StreamEntryDeletionResult} for each ID: {@link StreamEntryDeletionResult#NOT_FOUND} if no such ID exists,
	 *         {@link StreamEntryDeletionResult#DELETED} if the entry was deleted, {@link StreamEntryDeletionResult#NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED}
	 *         if the entry was not deleted but there are still dangling references (ACKED deletion policy).
	 *         Returns {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xdelex">Redis Documentation: XDELEX</a>
	 */
	default List<StreamEntryDeletionResult> xDelEx(byte @NonNull [] key, XDelOptions options, @NonNull String @NonNull... recordIds) {
		return xDelEx(key, options, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Deletes one or multiple entries from the stream at the specified key.
	 * <p>
	 * XDELEX is an extension of the Redis Streams XDEL command that provides more control over how message entries
	 * are deleted concerning consumer groups.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param options the {@link XDelOptions} specifying deletion policy. Use {@link XDelOptions#defaults()} ()} for default behavior.
	 * @param recordIds the id's of the records to remove.
	 * @return list of {@link StreamEntryDeletionResult} for each ID: {@link StreamEntryDeletionResult#NOT_FOUND} if no such ID exists,
	 *         {@link StreamEntryDeletionResult#DELETED} if the entry was deleted, {@link StreamEntryDeletionResult#NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED}
	 *         if the entry was not deleted but there are still dangling references (ACKED deletion policy).
	 *         Returns {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xdelex">Redis Documentation: XDELEX</a>
	 */
	List<StreamEntryDeletionResult> xDelEx(byte @NonNull [] key, XDelOptions options, @NonNull RecordId @NonNull... recordIds);

	/**
	 * Acknowledges and conditionally deletes one or multiple entries (messages) for a stream consumer group at the specified key.
	 * <p>
	 * XACKDEL combines the functionality of XACK and XDEL in Redis Streams. It acknowledges the specified entry IDs in the
	 * given consumer group and simultaneously attempts to delete the corresponding entries from the stream.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the consumer group.
	 * @param options the {@link XDelOptions} specifying deletion policy. Use {@link XDelOptions#defaults()} ()} for default behavior.
	 * @param recordIds the id's of the records to acknowledge and remove.
	 * @return list of {@link StreamEntryDeletionResult} for each ID: {@link StreamEntryDeletionResult#DELETED} if the entry was acknowledged and deleted,
	 *         {@link StreamEntryDeletionResult#NOT_FOUND} if no such ID exists, {@link StreamEntryDeletionResult#NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED}
	 *         if the entry was acknowledged but not deleted (when using ACKED deletion policy).
	 *         Returns {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xackdel">Redis Documentation: XACKDEL</a>
	 */
	default List<StreamEntryDeletionResult> xAckDel(byte @NonNull [] key, @NonNull String group, XDelOptions options, @NonNull String @NonNull... recordIds) {
		return xAckDel(key, group, options, Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
	}

	/**
	 * Acknowledges and conditionally deletes one or multiple entries (messages) for a stream consumer group at the specified key.
	 * <p>
	 * XACKDEL combines the functionality of XACK and XDEL in Redis Streams. It acknowledges the specified entry IDs in the
	 * given consumer group and simultaneously attempts to delete the corresponding entries from the stream.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group name of the consumer group.
	 * @param options the {@link XDelOptions} specifying deletion policy. Use {@link XDelOptions#defaults()} ()} for default behavior.
	 * @param recordIds the id's of the records to acknowledge and remove.
	 * @return list of {@link StreamEntryDeletionResult} for each ID: {@link StreamEntryDeletionResult#DELETED} if the entry was acknowledged and deleted,
	 *         {@link StreamEntryDeletionResult#NOT_FOUND} if no such ID exists, {@link StreamEntryDeletionResult#NOT_DELETED_UNACKNOWLEDGED_OR_STILL_REFERENCED}
	 *         if the entry was acknowledged but not deleted (when using ACKED deletion policy).
	 *         Returns {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xackdel">Redis Documentation: XACKDEL</a>
	 */
	List<StreamEntryDeletionResult> xAckDel(byte @NonNull [] key, @NonNull String group, XDelOptions options, @NonNull RecordId @NonNull... recordIds);
	/**
	 * Create a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group to create.
	 * @param readOffset the offset to start at.
	 * @return {@literal ok} if successful. {@literal null} when used in pipeline / transaction.
	 */
	String xGroupCreate(byte @NonNull [] key, @NonNull String groupName, @NonNull ReadOffset readOffset);

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
	String xGroupCreate(byte @NonNull [] key, @NonNull String groupName, @NonNull ReadOffset readOffset,
			boolean mkStream);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName the name of the group to remove the consumer from.
	 * @param consumerName the name of the consumer to remove from the group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	default Boolean xGroupDelConsumer(byte @NonNull [] key, @NonNull String groupName, @NonNull String consumerName) {
		return xGroupDelConsumer(key, Consumer.from(groupName, consumerName));
	}

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param consumer consumer identified by group name and consumer name.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Boolean xGroupDelConsumer(byte @NonNull [] key, @NonNull Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Boolean xGroupDestroy(byte @NonNull [] key, @NonNull String groupName);

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoStream xInfo(byte @NonNull [] key);

	/**
	 * Obtain information about {@literal consumer groups} associated with the stream stored at the specified
	 * {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoGroups xInfoGroups(byte @NonNull [] key);

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the {@literal consumer group}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoConsumers xInfoConsumers(byte @NonNull [] key, @NonNull String groupName);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Long xLen(byte @NonNull [] key);

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
	PendingMessagesSummary xPending(byte @NonNull [] key, @NonNull String groupName);

	/**
	 * Obtained detailed information about all pending messages for a given {@link Consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default PendingMessages xPending(byte @NonNull [] key, @NonNull Consumer consumer) {
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
	default PendingMessages xPending(byte @NonNull [] key, @NonNull String groupName, @NonNull String consumerName) {
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
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default PendingMessages xPending(byte @NonNull [] key, @NonNull String groupName, @NonNull Range<?> range,
			@NonNull Long count) {
		return xPending(key, groupName, XPendingOptions.range(range, count));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} within a
	 * {@literal consumer group} and over a given {@link Duration} of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param idle the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	default PendingMessages xPending(byte[] key, String groupName, Range<?> range, Long count, Duration idle) {
		return xPending(key, groupName, XPendingOptions.range(range, count).minIdleTime(idle));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default PendingMessages xPending(byte @NonNull [] key, @NonNull Consumer consumer, @NonNull Range<?> range,
			@NonNull Long count) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group} and over a given {@link Duration} of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param minIdleTime the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	default PendingMessages xPending(byte[] key, Consumer consumer, Range<?> range, Long count, Duration minIdleTime) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count, minIdleTime);
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
	default PendingMessages xPending(byte @NonNull [] key, @NonNull String groupName, @NonNull String consumerName,
			@NonNull Range<?> range, @NonNull Long count) {
		return xPending(key, groupName, XPendingOptions.range(range, count).consumer(consumerName));
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@literal consumer} within a {@literal consumer group} and over a given {@link Duration} of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the name of the {@literal consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param idle the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer} in given {@literal consumer group} or {@literal null}
	 *         when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	default PendingMessages xPending(byte[] key, String groupName, String consumerName, Range<?> range, Long count,
			Duration idle) {
		return xPending(key, groupName, XPendingOptions.range(range, count).consumer(consumerName).minIdleTime(idle));
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
	PendingMessages xPending(byte @NonNull [] key, @NonNull String groupName, @NonNull XPendingOptions options);

	/**
	 * Value Object holding parameters for obtaining pending messages.
	 *
	 * @author Christoph Strobl
	 * @author Jeonggyu Choi
	 * @since 2.3
	 */
	@NullMarked
	class XPendingOptions {

		private final @Nullable String consumerName;
		private final Range<?> range;
		private final @Nullable Long count;
		private final @Nullable Duration minIdleTime;

		private XPendingOptions(@Nullable String consumerName, Range<?> range, @Nullable Long count,
				@Nullable Duration minIdleTime) {

			this.range = range;
			this.count = count;
			this.consumerName = consumerName;
			this.minIdleTime = minIdleTime;
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
		 * @param count the max number of messages to return. Must not be negative.
		 * @return new instance of {@link XPendingOptions}.
		 */
		public static XPendingOptions unbounded(Long count) {

			Assert.isTrue(count > -1, "Count must not be negative");

			return new XPendingOptions(null, Range.unbounded(), count, null);
		}

		/**
		 * Create new {@link XPendingOptions} with given {@link Range} and limit.
		 *
		 * @param range must not be {@literal null}.
		 * @param count the max number of messages to return. Must not be negative.
		 * @return new instance of {@link XPendingOptions}.
		 */
		public static XPendingOptions range(Range<?> range, Long count) {

			Assert.notNull(range, "Range must not be null");
			Assert.isTrue(count > -1, "Count must not be negative");

			return new XPendingOptions(null, range, count, null);
		}

		/**
		 * Append given consumer.
		 *
		 * @param consumerName must not be {@literal null}.
		 * @return new instance of {@link XPendingOptions}.
		 */
		public XPendingOptions consumer(String consumerName) {

			Assert.notNull(consumerName, "Consumer name must not be null");

			return new XPendingOptions(consumerName, range, count, minIdleTime);
		}

		/**
		 * Append given minimum idle time.
		 *
		 * @param minIdleTime can be {@literal null} for none.
		 * @return new instance of {@link XPendingOptions}.
		 * @since 4.0
		 */
		public XPendingOptions minIdleTime(@Nullable Duration minIdleTime) {

			Assert.notNull(minIdleTime, "Idle must not be null");

			return new XPendingOptions(consumerName, range, count, minIdleTime);
		}

		XPendingOptions withRange(Range<?> range, Long count) {
			return new XPendingOptions(consumerName, range, count, minIdleTime);
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
		public @Nullable Long getCount() {
			return count;
		}

		/**
		 * @return can be {@literal null}.
		 */
		public @Nullable String getConsumerName() {
			return consumerName;
		}

		/**
		 * @return can be {@literal null}.
		 * @since 4.0
		 */
		public @Nullable Duration getMinIdleTime() {
			return minIdleTime;
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
			return count != null;
		}

		/**
		 * @return {@literal true} if idle time is set.
		 */
		public boolean hasMinIdleTime() {
			return minIdleTime != null;
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
	default List<@NonNull ByteRecord> xRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range) {
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
	List<@NonNull ByteRecord> xRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range, @NonNull Limit limit);

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default List<@NonNull ByteRecord> xRead(StreamOffset<byte @NonNull []> @NonNull... streams) {
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
	List<@NonNull ByteRecord> xRead(@NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<byte[]> @NonNull... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default List<@NonNull ByteRecord> xReadGroup(@NonNull Consumer consumer,
			@NonNull StreamOffset<byte[]> @NonNull... streams) {
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
	List<@NonNull ByteRecord> xReadGroup(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<byte[]> @NonNull... streams);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default List<@NonNull ByteRecord> xRevRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range) {
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
	List<@NonNull ByteRecord> xRevRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range,
			@NonNull Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long xTrim(byte @NonNull [] key, long count);

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
	Long xTrim(byte @NonNull [] key, long count, boolean approximateTrimming);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param options the trimming options.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long xTrim(byte @NonNull [] key, @NonNull XTrimOptions options);
}
