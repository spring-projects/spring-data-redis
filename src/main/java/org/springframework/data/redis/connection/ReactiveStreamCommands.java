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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.ByteBufferRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumer;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroup;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Stream-specific Redis commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tugdual Grall
 * @author Dengliming
 * @since 2.2
 */
public interface ReactiveStreamCommands {

	/**
	 * {@code XACK} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	class AcknowledgeCommand extends KeyCommand {

		private final @Nullable String group;
		private final List<RecordId> recordIds;

		private AcknowledgeCommand(@Nullable ByteBuffer key, @Nullable String group, List<RecordId> recordIds) {

			super(key);
			this.group = group;
			this.recordIds = recordIds;
		}

		/**
		 * Creates a new {@link AcknowledgeCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} for {@link ByteBuffer key}.
		 */
		public static AcknowledgeCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new AcknowledgeCommand(key, null, Collections.emptyList());
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal recordIds} applied.
		 */
		public AcknowledgeCommand forRecords(String... recordIds) {

			Assert.notNull(recordIds, "recordIds must not be null!");

			return forRecords(Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal recordIds} applied.
		 */
		public AcknowledgeCommand forRecords(RecordId... recordIds) {

			Assert.notNull(recordIds, "recordIds must not be null!");

			List<RecordId> newrecordIds = new ArrayList<>(getRecordIds().size() + recordIds.length);
			newrecordIds.addAll(getRecordIds());
			newrecordIds.addAll(Arrays.asList(recordIds));

			return new AcknowledgeCommand(getKey(), getGroup(), newrecordIds);
		}

		/**
		 * Applies the {@literal group}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param group must not be {@literal null}.
		 * @return a new {@link AcknowledgeCommand} with {@literal group} applied.
		 */
		public AcknowledgeCommand inGroup(String group) {

			Assert.notNull(group, "Group must not be null!");

			return new AcknowledgeCommand(getKey(), group, getRecordIds());
		}

		@Nullable
		public String getGroup() {
			return group;
		}

		public List<RecordId> getRecordIds() {
			return recordIds;
		}
	}

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record Id's to acknowledge.
	 * @return {@link Mono} emitting the nr of acknowledged messages.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<Long> xAck(ByteBuffer key, String group, String... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		return xAck(Mono.just(AcknowledgeCommand.stream(key).inGroup(group).forRecords(recordIds))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param recordIds record Id's to acknowledge.
	 * @return {@link Mono} emitting the nr of acknowledged messages.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<Long> xAck(ByteBuffer key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		return xAck(Mono.just(AcknowledgeCommand.stream(key).inGroup(group).forRecords(recordIds))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Acknowledge one or more records as processed.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the nr of acknowledged messages per {@link AcknowledgeCommand}.
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	Flux<NumericResponse<AcknowledgeCommand, Long>> xAck(Publisher<AcknowledgeCommand> commands);

	/**
	 * {@code XADD} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	class AddStreamRecord extends KeyCommand {

		private final ByteBufferRecord record;
		private final @Nullable Long maxlen;

		private AddStreamRecord(ByteBufferRecord record, @Nullable Long maxlen) {

			super(record.getStream());
			this.record = record;
			this.maxlen = maxlen;
		}

		/**
		 * Creates a new {@link AddStreamRecord} given {@link Map body}.
		 *
		 * @param record must not be {@literal null}.
		 * @return a new {@link AddStreamRecord}.
		 */
		public static AddStreamRecord of(ByteBufferRecord record) {

			Assert.notNull(record, "Record must not be null!");

			return new AddStreamRecord(record, null);
		}

		/**
		 * Creates a new {@link AddStreamRecord} given {@link Map body}.
		 *
		 * @param body must not be {@literal null}.
		 * @return a new {@link AddStreamRecord} for {@link Map}.
		 */
		public static AddStreamRecord body(Map<ByteBuffer, ByteBuffer> body) {

			Assert.notNull(body, "Body must not be null!");

			return new AddStreamRecord(StreamRecords.rawBuffer(body), null);
		}

		/**
		 * Applies the Geo set {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ReactiveGeoCommands.GeoAddCommand} with {@literal key} applied.
		 */
		public AddStreamRecord to(ByteBuffer key) {
			return new AddStreamRecord(record.withStreamKey(key), maxlen);
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return new instance of {@link AddStreamRecord}.
		 */
		public AddStreamRecord maxlen(long maxlen) {
			return new AddStreamRecord(record, maxlen);
		}

		/**
		 * @return the actual {@link ByteBufferRecord#getValue()}
		 */
		public Map<ByteBuffer, ByteBuffer> getBody() {
			return record.getValue();
		}

		public ByteBufferRecord getRecord() {
			return record;
		}

		/**
		 * Limit the size of the stream to the given maximum number of elements.
		 *
		 * @return can be {@literal null}.
		 * @since 2.3
		 */
		@Nullable
		public Long getMaxlen() {
			return maxlen;
		}

		/**
		 * @return {@literal true} if {@literal MAXLEN} is set.
		 * @since 2.3
		 */
		public boolean hasMaxlen() {
			return maxlen != null && maxlen > 0;
		}
	}

	/**
	 * Add stream record with given {@literal body} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param body must not be {@literal null}.
	 * @return {@link Mono} emitting the server generated {@link RecordId id}.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<RecordId> xAdd(ByteBuffer key, Map<ByteBuffer, ByteBuffer> body) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(body, "Body must not be null!");

		return xAdd(StreamRecords.newRecord().in(key).ofBuffer(body));
	}

	/**
	 * Add stream record with given {@literal body} to {@literal key}.
	 *
	 * @param record must not be {@literal null}.
	 * @return {@link Mono} the {@link RecordId id}.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default Mono<RecordId> xAdd(ByteBufferRecord record) {

		Assert.notNull(record, "Record must not be null!");

		return xAdd(Mono.just(AddStreamRecord.of(record))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Add stream record with given {@literal body} to {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the {@link RecordId} on by for for the given {@link AddStreamRecord} commands.
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	Flux<CommandResponse<AddStreamRecord, RecordId>> xAdd(Publisher<AddStreamRecord> commands);

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer} without increasing the delivered
	 * count.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param options must not be {@literal null}.
	 * @return a {@link Flux} emitting {@link RecordId is} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	default Flux<RecordId> xClaimJustId(ByteBuffer key, String group, String newOwner, XClaimOptions options) {

		return xClaimJustId(Mono.just(new XClaimCommand(key, group, newOwner, options))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer} without increasing the delivered
	 * count.
	 *
	 * @param commands must not be {@literal null}.
	 * @return a {@link Flux} emitting {@link RecordId is} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	Flux<CommandResponse<XClaimCommand, Flux<RecordId>>> xClaimJustId(Publisher<XClaimCommand> commands);

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param minIdleTime must not be {@literal null}.
	 * @param recordIds must not be {@literal null}.
	 * @return a {@link Flux} emitting {@link ByteBufferRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	default Flux<ByteBufferRecord> xClaim(ByteBuffer key, String group, String newOwner, Duration minIdleTime,
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
	 * @return a {@link Flux} emitting {@link ByteBufferRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	default Flux<ByteBufferRecord> xClaim(ByteBuffer key, String group, String newOwner, XClaimOptions options) {

		return xClaim(Mono.just(new XClaimCommand(key, group, newOwner, options))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	Flux<CommandResponse<XClaimCommand, Flux<ByteBufferRecord>>> xClaim(Publisher<XClaimCommand> commands);

	/**
	 * {@code XCLAIM} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	class XClaimCommand extends KeyCommand {

		private final String groupName;
		private final String newOwner;
		private final XClaimOptions options;

		private XClaimCommand(@Nullable ByteBuffer key, String groupName, String newOwner, XClaimOptions options) {

			super(key);
			this.groupName = groupName;
			this.newOwner = newOwner;
			this.options = options;
		}

		public XClaimOptions getOptions() {
			return options;
		}

		public String getNewOwner() {
			return newOwner;
		}

		public String getGroupName() {
			return groupName;
		}
	}

	/**
	 * {@code XDEL} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	class DeleteCommand extends KeyCommand {

		private final List<RecordId> recordIds;

		private DeleteCommand(@Nullable ByteBuffer key, List<RecordId> recordIds) {

			super(key);
			this.recordIds = recordIds;
		}

		/**
		 * Creates a new {@link DeleteCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link DeleteCommand} for {@link ByteBuffer key}.
		 */
		public static DeleteCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new DeleteCommand(key, Collections.emptyList());
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link DeleteCommand} with {@literal recordIds} applied.
		 */
		public DeleteCommand records(String... recordIds) {

			Assert.notNull(recordIds, "RecordIds must not be null!");

			return records(Arrays.stream(recordIds).map(RecordId::of).toArray(RecordId[]::new));
		}

		/**
		 * Applies the {@literal recordIds}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param recordIds must not be {@literal null}.
		 * @return a new {@link DeleteCommand} with {@literal recordIds} applied.
		 */
		public DeleteCommand records(RecordId... recordIds) {

			Assert.notNull(recordIds, "RecordIds must not be null!");

			List<RecordId> newrecordIds = new ArrayList<>(getRecordIds().size() + recordIds.length);
			newrecordIds.addAll(getRecordIds());
			newrecordIds.addAll(Arrays.asList(recordIds));

			return new DeleteCommand(getKey(), newrecordIds);
		}

		public List<RecordId> getRecordIds() {
			return recordIds;
		}
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return {@link Mono} emitting the number of removed entries.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> xDel(ByteBuffer key, String... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "RecordIds must not be null!");

		return xDel(Mono.just(DeleteCommand.stream(key).records(recordIds))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param recordIds stream record Id's.
	 * @return {@link Mono} emitting the number of removed entries.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Mono<Long> xDel(ByteBuffer key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "RecordIds must not be null!");

		return xDel(Mono.just(DeleteCommand.stream(key).records(recordIds))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Mono} emitting the number of removed entries.
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	Flux<CommandResponse<DeleteCommand, Long>> xDel(Publisher<DeleteCommand> commands);

	/**
	 * Get the size of the stream stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@link Mono} emitting the length of the stream.
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	default Mono<Long> xLen(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return xLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of the stream stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the length of the stream per {@link KeyCommand}.
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> xLen(Publisher<KeyCommand> commands);

	/**
	 * Obtain the {@link PendingMessagesSummary} for a given {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @return {@link Mono} emitting a summary of pending messages within the given {@literal consumer group}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessagesSummary> xPending(ByteBuffer key, String groupName) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(groupName, "GroupName must not be null!");

		return xPendingSummary(Mono.just(new PendingRecordsCommand(key, groupName, null, Range.unbounded(), null, null)))
				.next().map(CommandResponse::getOutput);
	}

	/**
	 * Obtain the {@link PendingMessagesSummary} for a given {@literal consumer group}.
	 *
	 * @param commands must not be {@literal null}..
	 * @return {@link Flux} emitting a summary of pending messages within the given {@literal consumer group} one by one.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	Flux<CommandResponse<PendingRecordsCommand, PendingMessagesSummary>> xPendingSummary(
			Publisher<PendingRecordsCommand> commands);

	/**
	 * Obtained detailed information about all pending messages for a given {@link Consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return {@link Mono} emitting pending messages for the given {@link Consumer}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default Mono<PendingMessages> xPending(ByteBuffer key, Consumer consumer) {
		return xPending(key, consumer.getGroup(), consumer.getName());
	}

	/**
	 * Obtained detailed information about all pending messages for a given {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	 * @return {@link Mono} emitting pending messages for the given {@link Consumer}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	@Nullable
	default Mono<PendingMessages> xPending(ByteBuffer key, String groupName, String consumerName) {
		return xPending(Mono.just(new PendingRecordsCommand(key, groupName, consumerName, Range.unbounded(), null, null)))
				.next().map(CommandResponse::getOutput);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} within a
	 * {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return {@link Mono} emitting pending messages for the given {@literal consumer group}. transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessages> xPending(ByteBuffer key, String groupName, Range<?> range, Long count) {
		return xPending(Mono.just(new PendingRecordsCommand(key, groupName, null, range, count, null))).next()
				.map(CommandResponse::getOutput);
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
	 * @return {@link Mono} emitting pending messages for the given {@literal consumer group}. transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessages> xPending(ByteBuffer key, String groupName, Range<?> range, Long count,
			Long idleMilliSeconds) {
		return xPending(Mono.just(new PendingRecordsCommand(key, groupName, null, range, count, idleMilliSeconds))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given {@link Range} and
	 * {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return {@link Mono} emitting pending messages for the given {@link Consumer}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessages> xPending(ByteBuffer key, Consumer consumer, Range<?> range, Long count) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count);
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
	 * @return {@link Mono} emitting pending messages for the given {@literal consumer} in given
	 *         {@literal consumer group}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessages> xPending(ByteBuffer key, String groupName, String consumerName, Range<?> range,
			Long count) {
		return xPending(Mono.just(new PendingRecordsCommand(key, groupName, consumerName, range, count, null))).next()
				.map(CommandResponse::getOutput);
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
	 * @param idleMilliSeconds the pending messages which were idle for certain duration . Must not be {@literal null}.
	 * @return {@link Mono} emitting pending messages for the given {@literal consumer} in given
	 *         {@literal consumer group}.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	default Mono<PendingMessages> xPending(ByteBuffer key, String groupName, String consumerName, Range<?> range,
			Long count, Long idleMilliSeconds) {
		return xPending(Mono.just(new PendingRecordsCommand(key, groupName, consumerName, range, count, idleMilliSeconds)))
				.next().map(CommandResponse::getOutput);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} applying given {@link XPendingOptions
	 * options}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting pending messages matching given criteria.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	Flux<CommandResponse<PendingRecordsCommand, PendingMessages>> xPending(Publisher<PendingRecordsCommand> commands);

	/**
	 * Value Object holding parameters for obtaining pending messages.
	 *
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	class PendingRecordsCommand extends KeyCommand {

		private final String groupName;
		private final @Nullable String consumerName;
		private final Range<?> range;
		private final @Nullable Long count;
		private final @Nullable Long idleMilliSeconds;

		private PendingRecordsCommand(ByteBuffer key, String groupName, @Nullable String consumerName, Range<?> range,
				@Nullable Long count, @Nullable Long idleMilliSeconds) {

			super(key);

			this.groupName = groupName;
			this.consumerName = consumerName;
			this.range = range;
			this.count = count;
			this.idleMilliSeconds = idleMilliSeconds;
		}

		/**
		 * Create new unbounded {@link PendingRecordsCommand}.
		 *
		 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
		 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
		 * @return new instance of {@link PendingRecordsCommand}.
		 */
		static PendingRecordsCommand pending(ByteBuffer key, String groupName) {
			return new PendingRecordsCommand(key, groupName, null, Range.unbounded(), null, null);
		}

		/**
		 * Create new {@link PendingRecordsCommand} with given {@link Range},limit and messages which are idle for certain
		 * time.
		 *
		 * @return new instance of {@link XPendingOptions}.
		 */
		public PendingRecordsCommand range(Range<String> range, Long count, Long idleMilliSeconds) {
			return new PendingRecordsCommand(getKey(), groupName, consumerName, range, count, idleMilliSeconds);
		}

		/**
		 * Append given consumer.
		 *
		 * @param consumerName must not be {@literal null}.
		 * @return new instance of {@link PendingRecordsCommand}.
		 */
		public PendingRecordsCommand consumer(String consumerName) {
			return new PendingRecordsCommand(getKey(), groupName, consumerName, range, count, idleMilliSeconds);
		}

		public String getGroupName() {
			return groupName;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public String getConsumerName() {
			return consumerName;
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
	}

	/**
	 * {@code XRANGE}/{@code XREVRANGE} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	class RangeCommand extends KeyCommand {

		private final Range<String> range;
		private final Limit limit;

		/**
		 * Creates a new {@link RangeCommand} given a {@code key}, {@link Range}, and {@link Limit}.
		 *
		 * @param key must not be {@literal null}.
		 * @param range must not be {@literal null}.
		 * @param limit must not be {@literal null}.
		 */
		private RangeCommand(ByteBuffer key, Range<String> range, Limit limit) {

			super(key);
			this.range = range;
			this.limit = limit;
		}

		/**
		 * Creates a new {@link RangeCommand} given a {@code key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link RangeCommand} for {@code key}.
		 */
		public static RangeCommand stream(ByteBuffer key) {
			return new RangeCommand(key, Range.unbounded(), Limit.unlimited());
		}

		/**
		 * Applies a {@link Range}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link RangeCommand} with {@link Range} applied.
		 */
		public RangeCommand within(Range<String> range) {

			Assert.notNull(range, "Range must not be null!");

			return new RangeCommand(getKey(), range, getLimit());
		}

		/**
		 * Applies a {@code Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param count
		 * @return a new {@link RangeCommand} with {@code limit} applied.
		 */
		public RangeCommand limit(int count) {
			return new RangeCommand(getKey(), range, Limit.unlimited().count(count));
		}

		/**
		 * Applies a {@code Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param limit must not be {@literal null}.
		 * @return a new {@link RangeCommand} with {@code limit} applied.
		 */
		public RangeCommand limit(Limit limit) {

			Assert.notNull(limit, "Limit must not be null!");

			return new RangeCommand(getKey(), range, limit);
		}

		/**
		 * @return the {@link Range}.
		 */
		public Range<String> getRange() {
			return range;
		}

		/**
		 * @return the {@link Limit}.
		 */
		public Limit getLimit() {
			return limit;
		}
	}

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return {@link Flux} emitting with members of the stream.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRange(ByteBuffer key, Range<String> range) {
		return xRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@link Flux} emitting with members of the stream.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRange(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return xRange(Mono.just(RangeCommand.stream(key).within(range).limit(limit))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting with members of the stream per {@link RangeCommand}.
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	Flux<CommandResponse<RangeCommand, Flux<ByteBufferRecord>>> xRange(Publisher<RangeCommand> commands);

	/**
	 * {@code XRANGE}/{@code XREVRANGE} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	class ReadCommand {

		private final List<StreamOffset<ByteBuffer>> streamOffsets;
		private final @Nullable StreamReadOptions readOptions;
		private final @Nullable Consumer consumer;

		/**
		 * @param streamOffsets must not be {@literal null}.
		 * @param readOptions
		 * @param consumer
		 */
		public ReadCommand(List<StreamOffset<ByteBuffer>> streamOffsets, @Nullable StreamReadOptions readOptions,
				@Nullable Consumer consumer) {

			this.readOptions = readOptions;
			this.consumer = consumer;
			this.streamOffsets = streamOffsets;
		}

		/**
		 * Creates a new {@link ReadCommand} given a {@link StreamOffset}.
		 *
		 * @param streamOffset must not be {@literal null}.
		 * @return a new {@link ReadCommand} for {@link StreamOffset}.
		 */
		public static ReadCommand from(StreamOffset<ByteBuffer> streamOffset) {

			Assert.notNull(streamOffset, "StreamOffset must not be null!");

			return new ReadCommand(Collections.singletonList(streamOffset), StreamReadOptions.empty(), null);
		}

		/**
		 * Creates a new {@link ReadCommand} given a {@link StreamOffset}s.
		 *
		 * @param streamOffsets must not be {@literal null}.
		 * @return a new {@link ReadCommand} for {@link StreamOffset}s.
		 */
		public static ReadCommand from(StreamOffset<ByteBuffer>... streamOffsets) {

			Assert.notNull(streamOffsets, "StreamOffsets must not be null!");

			return new ReadCommand(Arrays.asList(streamOffsets), StreamReadOptions.empty(), null);
		}

		/**
		 * Applies a {@link Consumer}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param consumer must not be {@literal null}.
		 * @return a new {@link ReadCommand} with {@link Consumer} applied.
		 */
		public ReadCommand as(Consumer consumer) {

			Assert.notNull(consumer, "Consumer must not be null!");

			return new ReadCommand(getStreamOffsets(), getReadOptions(), consumer);
		}

		/**
		 * Applies the given {@link StreamReadOptions}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param options must not be {@literal null}.
		 * @return a new {@link ReadCommand} with {@link Consumer} applied.
		 */
		public ReadCommand withOptions(StreamReadOptions options) {

			Assert.notNull(options, "StreamReadOptions must not be null!");

			return new ReadCommand(getStreamOffsets(), options, getConsumer());
		}

		public List<StreamOffset<ByteBuffer>> getStreamOffsets() {
			return streamOffsets;
		}

		@Nullable
		public StreamReadOptions getReadOptions() {
			return readOptions;
		}

		@Nullable
		public Consumer getConsumer() {
			return consumer;
		}
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return {@link Flux} emitting with members of the stream.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<ByteBufferRecord> xRead(StreamOffset<ByteBuffer>... streams) {
		return xRead(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return {@link Flux} emitting with members of the stream.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default Flux<ByteBufferRecord> xRead(StreamReadOptions readOptions, StreamOffset<ByteBuffer>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		return read(Mono.just(ReadCommand.from(streams).withOptions(readOptions))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the members of the stream per {@link ReadCommand}.
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	Flux<CommandResponse<ReadCommand, Flux<ByteBufferRecord>>> read(Publisher<ReadCommand> commands);

	/**
	 * @author Christoph Strobl
	 * @since 2.3
	 */
	class XInfoCommand extends KeyCommand {

		private final @Nullable String groupName;

		private XInfoCommand(ByteBuffer key, @Nullable String groupName) {

			super(key);
			this.groupName = groupName;
		}

		public static XInfoCommand of(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new XInfoCommand(key, null);
		}

		public XInfoCommand consumersIn(String groupName) {
			return new XInfoCommand(getKey(), groupName);
		}

		@Nullable
		public String getGroupName() {
			return groupName;
		}
	}

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return a {@link Mono} emitting {@link XInfoStream} when ready.
	 * @since 2.3
	 */
	default Mono<XInfoStream> xInfo(ByteBuffer key) {
		return xInfo(Mono.just(XInfoCommand.of(key))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.3
	 */
	Flux<CommandResponse<XInfoCommand, XInfoStream>> xInfo(Publisher<XInfoCommand> commands);

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return a {@link Flux} emitting consumer group info one by one.
	 * @since 2.3
	 */
	default Flux<XInfoGroup> xInfoGroups(ByteBuffer key) {
		return xInfoGroups(Mono.just(XInfoCommand.of(key))).next().flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.3
	 */
	Flux<CommandResponse<XInfoCommand, Flux<XInfoGroup>>> xInfoGroups(Publisher<XInfoCommand> commands);

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the {@literal consumer group}.
	 * @return a {@link Flux} emitting consumer info one by one.
	 * @since 2.3
	 */
	default Flux<XInfoConsumer> xInfoConsumers(ByteBuffer key, String groupName) {
		return xInfoConsumers(Mono.just(XInfoCommand.of(key).consumersIn(groupName))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.3
	 */
	Flux<CommandResponse<XInfoCommand, Flux<XInfoConsumer>>> xInfoConsumers(Publisher<XInfoCommand> commands);

	class GroupCommand extends KeyCommand {

		private final GroupCommandAction action;
		private final @Nullable String groupName;
		private final @Nullable String consumerName;
		private final @Nullable ReadOffset offset;
		private final boolean mkStream;

		public GroupCommand(@Nullable ByteBuffer key, GroupCommandAction action, @Nullable String groupName,
				@Nullable String consumerName, @Nullable ReadOffset offset, boolean mkStream) {

			super(key);
			this.action = action;
			this.groupName = groupName;
			this.consumerName = consumerName;
			this.offset = offset;
			this.mkStream = mkStream;
		}

		public GroupCommand(@Nullable ByteBuffer key, GroupCommandAction action, @Nullable String groupName,
				@Nullable String consumerName, @Nullable ReadOffset offset) {
			this(key, action, groupName, consumerName, offset, false);
		}

		public static GroupCommand createGroup(String group) {
			return new GroupCommand(null, GroupCommandAction.CREATE, group, null, ReadOffset.latest());
		}

		public static GroupCommand destroyGroup(String group) {
			return new GroupCommand(null, GroupCommandAction.DESTROY, group, null, null);
		}

		public static GroupCommand deleteConsumer(String consumerName) {
			return new GroupCommand(null, GroupCommandAction.DELETE_CONSUMER, null, consumerName, null);
		}

		public static GroupCommand deleteConsumer(Consumer consumer) {
			return new GroupCommand(null, GroupCommandAction.DELETE_CONSUMER, consumer.getGroup(), consumer.getName(), null);
		}

		public GroupCommand makeStream(boolean mkStream) {
			return new GroupCommand(getKey(), action, groupName, consumerName, offset,mkStream);
		}

		public GroupCommand at(ReadOffset offset) {
			return new GroupCommand(getKey(), action, groupName, consumerName, offset);
		}

		public GroupCommand forStream(ByteBuffer key) {
			return new GroupCommand(key, action, groupName, consumerName, offset);
		}

		public GroupCommand fromGroup(String groupName) {
			return new GroupCommand(getKey(), action, groupName, consumerName, offset);
		}

		public boolean isMkStream() {
			return this.mkStream;
		}

		@Nullable
		public ReadOffset getReadOffset() {
			return this.offset;
		}

		@Nullable
		public String getGroupName() {
			return groupName;
		}

		@Nullable
		public String getConsumerName() {
			return consumerName;
		}

		public GroupCommandAction getAction() {
			return action;
		}

		public enum GroupCommandAction {
			CREATE, SET_ID, DESTROY, DELETE_CONSUMER
		}
	}

	/**
	 * Create a consumer group.
	 *
	 * @param key key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group to create.
	 * @param readOffset the offset to start at.
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 */
	default Mono<String> xGroupCreate(ByteBuffer key, String groupName, ReadOffset readOffset) {

		return xGroup(Mono.just(GroupCommand.createGroup(groupName).forStream(key).at(readOffset))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Create a consumer group.
	 *
	 * @param key key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group to create.
	 * @param readOffset the offset to start at.
	 * @param mkStream if true the group will create the stream if needed (MKSTREAM)
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 * @since 2.3
	 */
	default Mono<String> xGroupCreate(ByteBuffer key, String groupName, ReadOffset readOffset, boolean mkStream) {
		return xGroup(Mono.just(GroupCommand.createGroup(groupName).forStream(key).at(readOffset).makeStream(mkStream))).next()
				.map(CommandResponse::getOutput);
	}

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName the name of the group to remove the consumer from.
	 * @param consumerName the name of the consumer to remove from the group.
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 */
	@Nullable
	default Mono<String> xGroupDelConsumer(ByteBuffer key, String groupName, String consumerName) {
		return xGroupDelConsumer(key, Consumer.from(groupName, consumerName));
	}

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param consumer the {@link Consumer}.
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 */
	default Mono<String> xGroupDelConsumer(ByteBuffer key, Consumer consumer) {
		return xGroup(GroupCommand.deleteConsumer(consumer).forStream(key));
	}

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the consumer group.
	 * @return the {@link Mono} emitting {@literal ok} if successful.
	 */
	@Nullable
	default Mono<String> xGroupDestroy(ByteBuffer key, String groupName) {
		return xGroup(GroupCommand.destroyGroup(groupName).forStream(key));
	}

	/**
	 * Execute the given {@link GroupCommand} to {@literal create, destroy,... } groups.
	 *
	 * @param command the {@link GroupCommand} to run.
	 * @return the {@link Mono} emitting the command result.
	 */
	default Mono<String> xGroup(GroupCommand command) {
		return xGroup(Mono.just(command)).next().map(CommandResponse::getOutput);
	}

	/**
	 * Execute the given {@link GroupCommand} to {@literal create, destroy,... } groups.
	 *
	 * @param commands
	 * @return {@link Flux} emitting the results of the {@link GroupCommand} one by one.
	 */
	Flux<CommandResponse<GroupCommand, String>> xGroup(Publisher<GroupCommand> commands);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return {@link Flux} emitting the members of the stream
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<ByteBufferRecord> xReadGroup(Consumer consumer, StreamOffset<ByteBuffer>... streams) {
		return xReadGroup(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return {@link Flux} emitting the members of the stream.
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default Flux<ByteBufferRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<ByteBuffer>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		return read(Mono.just(ReadCommand.from(streams).withOptions(readOptions).as(consumer))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return {@link Flux} emitting the members of the stream in reverse.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRevRange(ByteBuffer key, Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@link Flux} emitting the members of the stream in reverse.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default Flux<ByteBufferRecord> xRevRange(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		return xRevRange(Mono.just(RangeCommand.stream(key).within(range).limit(limit))).next()
				.flatMapMany(CommandResponse::getOutput);
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a {@link Limit} in reverse order.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the members of the stream in reverse.
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	Flux<CommandResponse<RangeCommand, Flux<ByteBufferRecord>>> xRevRange(Publisher<RangeCommand> commands);

	/**
	 * {@code XTRIM} command parameters.
	 *
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	class TrimCommand extends KeyCommand {

		private @Nullable Long count;
		private boolean approximateTrimming;

		private TrimCommand(ByteBuffer key, @Nullable Long count, boolean approximateTrimming) {
			super(key);
			this.count = count;
			this.approximateTrimming = approximateTrimming;
		}

		/**
		 * Creates a new {@link TrimCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link TrimCommand} for {@link ByteBuffer key}.
		 */
		public static TrimCommand stream(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new TrimCommand(key, null, false);
		}

		/**
		 * Applies the numeric {@literal count}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param count
		 * @return a new {@link TrimCommand} with {@literal count} applied.
		 */
		public TrimCommand to(long count) {
			return new TrimCommand(getKey(), count, approximateTrimming);
		}

		/**
		 * Applies approximate trimming. Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link TrimCommand} with {@literal approximateTrimming} applied.
		 * @since 2.4
		 */
		public TrimCommand approximate() {
			return approximate(true);
		}

		/**
		 * Applies {@code approximateTrimming}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param approximateTrimming
		 * @return a new {@link TrimCommand} with {@literal approximateTrimming} applied.
		 * @since 2.4
		 */
		public TrimCommand approximate(boolean approximateTrimming) {
			return new TrimCommand(getKey(), count, approximateTrimming);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Long getCount() {
			return count;
		}

		public boolean isApproximateTrimming() {
			return approximateTrimming;
		}
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return {@link Mono} emitting the number of removed entries.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	default Mono<Long> xTrim(ByteBuffer key, long count) {
		return xTrim(key, count, false);
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @param approximateTrimming the trimming must be performed in a approximated way in order to maximize performances.
	 * @return {@link Mono} emitting the number of removed entries.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	default Mono<Long> xTrim(ByteBuffer key, long count, boolean approximateTrimming) {

		Assert.notNull(key, "Key must not be null!");

		return xTrim(Mono.just(TrimCommand.stream(key).to(count).approximate(approximateTrimming))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param commands must not be {@literal null}.
	 * @return {@link Flux} emitting the number of removed entries per {@link TrimCommand}.
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> xTrim(Publisher<TrimCommand> commands);
}
