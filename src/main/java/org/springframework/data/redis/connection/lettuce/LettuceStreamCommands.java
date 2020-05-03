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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @since 2.2
 */
@RequiredArgsConstructor
class LettuceStreamCommands implements RedisStreamCommands {

	private final @NonNull LettuceConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAck(byte[], byte[], java.lang.String[])
	 */
	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		String[] ids = entryIdsToString(recordIds);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xack(key, LettuceConverters.toBytes(group), ids)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xack(key, LettuceConverters.toBytes(group), ids)));
				return null;
			}
			return getConnection().xack(key, LettuceConverters.toBytes(group), ids);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAdd(byte[], MapRecord, XAddOptions)
	 */
	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {

		Assert.notNull(record.getStream(), "Stream must not be null!");
		Assert.notNull(record, "Record must not be null!");

		XAddArgs args = new XAddArgs();
		args.id(record.getId().getValue());
		if (options.hasMaxlen()) {
			args.maxlen(options.getMaxlen());
		}

		try {
			if (isPipelined()) {

				pipeline(connection.newLettuceResult(getAsyncConnection().xadd(record.getStream(), args, record.getValue()),
						RecordId::of));
				return null;
			}
			if (isQueueing()) {

				transaction(connection.newLettuceResult(getAsyncConnection().xadd(record.getStream(), args, record.getValue()),
						RecordId::of));
				return null;
			}

			return RecordId.of(getConnection().xadd(record.getStream(), args, record.getValue()));

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaimJustId(byte[], java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {

		String[] ids = options.getIdsAsStringArray();
		io.lettuce.core.Consumer<byte[]> from = io.lettuce.core.Consumer.from(LettuceConverters.toBytes(group),
				LettuceConverters.toBytes(newOwner));
		XClaimArgs args = StreamConverters.toXClaimArgs(options);

		if (true /* TODO: set the JUSTID flag */ ) {
			throw new UnsupportedOperationException("Lettuce does not support XCLAIM with JUSTID. (Ref: lettuce-io#1233)");
		}

		try {
			if (isPipelined()) {

				pipeline(connection.newLettuceResult(getAsyncConnection().xclaim(key, from, args, ids),
						StreamConverters.messagesToIds()));
				return null;
			}
			if (isQueueing()) {

				transaction(connection.newLettuceResult(getAsyncConnection().xclaim(key, from, args, ids),
						StreamConverters.messagesToIds()));
				return null;
			}

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}

		return StreamConverters.messagesToIds().convert(getConnection().xclaim(key, from, args, ids));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaim(byte[], java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {

		String[] ids = options.getIdsAsStringArray();
		io.lettuce.core.Consumer<byte[]> from = io.lettuce.core.Consumer.from(LettuceConverters.toBytes(group),
				LettuceConverters.toBytes(newOwner));
		XClaimArgs args = StreamConverters.toXClaimArgs(options);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xclaim(key, from, args, ids),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xclaim(key, from, args, ids),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			return StreamConverters.byteRecordListConverter().convert(getConnection().xclaim(key, from, args, ids));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xDel(byte[], java.lang.String[])
	 */
	@Override
	public Long xDel(byte[] key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xdel(key, entryIdsToString(recordIds))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xdel(key, entryIdsToString(recordIds))));
				return null;
			}
			return getConnection().xdel(key, entryIdsToString(recordIds));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupCreate(byte[], org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return xGroupCreate(key, groupName, readOffset, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupCreate(byte[], org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String, boolean)
	 */
	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkSteam) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");

		try {
			XReadArgs.StreamOffset<byte[]> streamOffset = XReadArgs.StreamOffset.from(key, readOffset.getOffset());

			if (isPipelined()) {
				pipeline(connection
						.newLettuceResult(getAsyncConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(groupName),
								XGroupCreateArgs.Builder.mkstream(mkSteam))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newLettuceResult(getAsyncConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(groupName),
								XGroupCreateArgs.Builder.mkstream(mkSteam))));
				return null;
			}
			return getConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(groupName), XGroupCreateArgs.Builder.mkstream(mkSteam));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDelConsumer(byte[], org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(consumer, "Consumer must not be null!");

		try {
			io.lettuce.core.Consumer<byte[]> lettuceConsumer = toConsumer(consumer);

			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xgroupDelconsumer(key, lettuceConsumer)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xgroupDelconsumer(key, lettuceConsumer)));
				return null;
			}
			return getConnection().xgroupDelconsumer(key, lettuceConsumer);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDestroy(byte[], java.lang.String)
	 */
	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().xgroupDestroy(key, LettuceConverters.toBytes(groupName))));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().xgroupDestroy(key, LettuceConverters.toBytes(groupName))));
				return null;
			}
			return getConnection().xgroupDestroy(key, LettuceConverters.toBytes(groupName));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfo(byte[])
	 */
	@Override
	public XInfoStream xInfo(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xinfoStream(key), XInfoStream::fromList));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xinfoStream(key), XInfoStream::fromList));
				return null;
			}
			return XInfoStream.fromList(getConnection().xinfoStream(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoGroups(byte[])
	 */
	@Override
	public XInfoGroups xInfoGroups(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xinfoGroups(key), XInfoGroups::fromList));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xinfoGroups(key), XInfoGroups::fromList));
				return null;
			}
			return XInfoGroups.fromList(getConnection().xinfoGroups(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoConsumers(byte[], java.lang.String)
	 */
	@Override
	public XInfoConsumers xInfoConsumers(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(groupName, "GroupName must not be null!");

		byte[] binaryGroupName = LettuceConverters.toBytes(groupName);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xinfoConsumers(key, binaryGroupName),
						it -> XInfoConsumers.fromList(groupName, it)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xinfoConsumers(key, binaryGroupName),
						it -> XInfoConsumers.fromList(groupName, it)));
				return null;
			}
			return XInfoConsumers.fromList(groupName, getConnection().xinfoConsumers(key, binaryGroupName));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xLen(byte[])
	 */
	@Override
	public Long xLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xlen(key)));
				return null;
			}
			return getConnection().xlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String)
	 */
	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {

		byte[] group = LettuceConverters.toBytes(groupName);
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xpending(key, group),
						it -> StreamConverters.toPendingMessagesInfo(groupName, it)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xpending(key, group),
						it -> StreamConverters.toPendingMessagesInfo(groupName, it)));
				return null;
			}
			return StreamConverters.toPendingMessagesInfo(groupName, getConnection().xpending(key, group));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions)
	 */
	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {

		byte[] group = LettuceConverters.toBytes(groupName);
		io.lettuce.core.Range<String> range = RangeConverter.toRangeWithDefault(options.getRange(), "-", "+");
		io.lettuce.core.Limit limit = options.isLimited() ? io.lettuce.core.Limit.from(options.getCount())
				: io.lettuce.core.Limit.unlimited();

		try {
			if (options.hasConsumer()) {

				if (isPipelined()) {
					pipeline(connection.newLettuceResult(getAsyncConnection().xpending(key,
							io.lettuce.core.Consumer.from(group, LettuceConverters.toBytes(options.getConsumerName())), range, limit),
							it -> StreamConverters.toPendingMessages(groupName, options.getRange(), it)));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(getAsyncConnection().xpending(key,
							io.lettuce.core.Consumer.from(group, LettuceConverters.toBytes(options.getConsumerName())), range, limit),
							it -> StreamConverters.toPendingMessages(groupName, options.getRange(), it)));
					return null;
				}
				return StreamConverters.toPendingMessages(groupName, options.getRange(), getConnection().xpending(key,
						io.lettuce.core.Consumer.from(group, LettuceConverters.toBytes(options.getConsumerName())), range, limit));

			} else {

				if (isPipelined()) {
					pipeline(connection.newLettuceResult(getAsyncConnection().xpending(key, group, range, limit),
							it -> StreamConverters.toPendingMessages(groupName, options.getRange(), it)));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(getAsyncConnection().xpending(key, group, range, limit),
							it -> StreamConverters.toPendingMessages(groupName, options.getRange(), it)));
					return null;
				}
				return StreamConverters.toPendingMessages(groupName, options.getRange(),
						getConnection().xpending(key, group, range, limit));
			}
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range, Function.identity());
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xrange(key, lettuceRange, lettuceLimit),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xrange(key, lettuceRange, lettuceLimit),
						StreamConverters.byteRecordListConverter()));
				return null;
			}

			return StreamConverters.byteRecordListConverter()
					.convert(getConnection().xrange(key, lettuceRange, lettuceLimit));

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRead(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		XReadArgs.StreamOffset<byte[]>[] streamOffsets = toStreamOffsets(streams);
		XReadArgs args = StreamConverters.toReadArgs(readOptions);

		if (isBlocking(readOptions)) {

			try {
				if (isPipelined()) {
					pipeline(connection.newLettuceResult(getAsyncDedicatedConnection().xread(args, streamOffsets),
							StreamConverters.byteRecordListConverter()));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(getAsyncDedicatedConnection().xread(args, streamOffsets),
							StreamConverters.byteRecordListConverter()));
					return null;
				}
				return StreamConverters.byteRecordListConverter().convert(getDedicatedConnection().xread(args, streamOffsets));
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xread(args, streamOffsets),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xread(args, streamOffsets),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			return StreamConverters.byteRecordListConverter().convert(getConnection().xread(args, streamOffsets));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xReadGroup(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		XReadArgs.StreamOffset<byte[]>[] streamOffsets = toStreamOffsets(streams);
		XReadArgs args = StreamConverters.toReadArgs(readOptions);
		io.lettuce.core.Consumer<byte[]> lettuceConsumer = toConsumer(consumer);

		if (isBlocking(readOptions)) {

			try {
				if (isPipelined()) {
					pipeline(connection.newLettuceResult(
							getAsyncDedicatedConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
							StreamConverters.byteRecordListConverter()));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(
							getAsyncDedicatedConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
							StreamConverters.byteRecordListConverter()));
					return null;
				}
				return StreamConverters.byteRecordListConverter()
						.convert(getDedicatedConnection().xreadgroup(lettuceConsumer, args, streamOffsets));
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			return StreamConverters.byteRecordListConverter()
					.convert(getConnection().xreadgroup(lettuceConsumer, args, streamOffsets));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRevRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range);
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xrevrange(key, lettuceRange, lettuceLimit),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xrevrange(key, lettuceRange, lettuceLimit),
						StreamConverters.byteRecordListConverter()));
				return null;
			}
			return StreamConverters.byteRecordListConverter()
					.convert(getConnection().xrevrange(key, lettuceRange, lettuceLimit));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xTrim(byte[], long)
	 */
	@Override
	public Long xTrim(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xtrim(key, count)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xtrim(key, count)));
				return null;
			}
			return getConnection().xtrim(key, count);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void pipeline(LettuceResult result) {
		connection.pipeline(result);
	}

	private void transaction(LettuceResult result) {
		connection.transaction(result);
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncDedicatedConnection() {
		return connection.getAsyncDedicatedConnection();
	}

	RedisClusterCommands<byte[], byte[]> getDedicatedConnection() {
		return connection.getDedicatedConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}

	private static boolean isBlocking(StreamReadOptions readOptions) {
		return readOptions.getBlock() != null && readOptions.getBlock() > 0;
	}

	@SuppressWarnings("unchecked")
	private static XReadArgs.StreamOffset<byte[]>[] toStreamOffsets(StreamOffset<byte[]>[] streams) {

		return Arrays.stream(streams).map(it -> XReadArgs.StreamOffset.from(it.getKey(), it.getOffset().getOffset()))
				.toArray(XReadArgs.StreamOffset[]::new);
	}

	private static io.lettuce.core.Consumer<byte[]> toConsumer(Consumer consumer) {

		return io.lettuce.core.Consumer.from(LettuceConverters.toBytes(consumer.getGroup()),
				LettuceConverters.toBytes(consumer.getName()));
	}

	private static String[] entryIdsToString(RecordId[] recordIds) {

		if (recordIds.length == 1) {
			return new String[] { recordIds[0].getValue() };
		}

		return Arrays.stream(recordIds).map(RecordId::getValue).toArray(String[]::new);
	}
}
