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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XPendingArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
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
 * @author Tugdual Grall
 * @author Dejan Jankov
 * @author Dengliming
 * @author Mark John Moreno
 * @author Jeonggyu Choi
 * @since 2.2
 */
class LettuceStreamCommands implements RedisStreamCommands {

	private final LettuceConnection connection;

	LettuceStreamCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(group, "Group name must not be null or empty");
		Assert.notNull(recordIds, "recordIds must not be null");

		String[] ids = entryIdsToString(recordIds);

		return connection.invoke().just(RedisStreamAsyncCommands::xack, key, LettuceConverters.toBytes(group), ids);
	}

	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {

		Assert.notNull(record.getStream(), "Stream must not be null");
		Assert.notNull(record, "Record must not be null");

		XAddArgs args = new XAddArgs();
		args.id(record.getId().getValue());
		if (options.hasMaxlen()) {
			args.maxlen(options.getMaxlen());
		}
		if (options.hasMinId()) {
			args.minId(options.getMinId().toString());
		}
		args.nomkstream(options.isNoMkStream());
		args.approximateTrimming(options.isApproximateTrimming());

		return connection.invoke().from(RedisStreamAsyncCommands::xadd, record.getStream(), args, record.getValue())
				.get(RecordId::of);
	}

	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {

		String[] ids = options.getIdsAsStringArray();
		io.lettuce.core.Consumer<byte[]> from = io.lettuce.core.Consumer.from(LettuceConverters.toBytes(group),
				LettuceConverters.toBytes(newOwner));
		XClaimArgs args = StreamConverters.toXClaimArgs(options).justid();

		return connection.invoke().fromMany(RedisStreamAsyncCommands::xclaim, key, from, args, ids)
				.toList(it -> RecordId.of(it.getId()));
	}

	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {

		String[] ids = options.getIdsAsStringArray();
		io.lettuce.core.Consumer<byte[]> from = io.lettuce.core.Consumer.from(LettuceConverters.toBytes(group),
				LettuceConverters.toBytes(newOwner));
		XClaimArgs args = StreamConverters.toXClaimArgs(options);

		return connection.invoke().fromMany(RedisStreamAsyncCommands::xclaim, key, from, args, ids)
				.toList(StreamConverters.byteRecordConverter());
	}

	@Override
	public Long xDel(byte[] key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(recordIds, "recordIds must not be null");

		return connection.invoke().just(RedisStreamAsyncCommands::xdel, key, entryIdsToString(recordIds));
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return xGroupCreate(key, groupName, readOffset, false);
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkSteam) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(groupName, "Group name must not be null or empty");
		Assert.notNull(readOffset, "ReadOffset must not be null");

		XReadArgs.StreamOffset<byte[]> streamOffset = XReadArgs.StreamOffset.from(key, readOffset.getOffset());

		return connection.invoke().just(RedisStreamAsyncCommands::xgroupCreate, streamOffset,
				LettuceConverters.toBytes(groupName), XGroupCreateArgs.Builder.mkstream(mkSteam));
	}

	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(consumer, "Consumer must not be null");

		io.lettuce.core.Consumer<byte[]> lettuceConsumer = toConsumer(consumer);

		return connection.invoke().from(RedisStreamAsyncCommands::xgroupDelconsumer, key, lettuceConsumer)
				.get(Objects::nonNull);
	}

	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(groupName, "Group name must not be null or empty");

		return connection.invoke().just(RedisStreamAsyncCommands::xgroupDestroy, key, LettuceConverters.toBytes(groupName));
	}

	@Override
	public XInfoStream xInfo(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisStreamAsyncCommands::xinfoStream, key).get(XInfoStream::fromList);
	}

	@Override
	public XInfoGroups xInfoGroups(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisStreamAsyncCommands::xinfoGroups, key).get(XInfoGroups::fromList);
	}

	@Override
	public XInfoConsumers xInfoConsumers(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(groupName, "GroupName must not be null");

		return connection.invoke().from(RedisStreamAsyncCommands::xinfoConsumers, key, LettuceConverters.toBytes(groupName))
				.get(it -> XInfoConsumers.fromList(groupName, it));
	}

	@Override
	public Long xLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStreamAsyncCommands::xlen, key);
	}

	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {

		byte[] group = LettuceConverters.toBytes(groupName);

		return connection.invoke().from(RedisStreamAsyncCommands::xpending, key, group)
				.get(it -> StreamConverters.toPendingMessagesInfo(groupName, it));
	}

	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {

		byte[] group = LettuceConverters.toBytes(groupName);
		io.lettuce.core.Range<String> range = RangeConverter.toRangeWithDefault(options.getRange(), "-", "+",
				Function.identity());
		io.lettuce.core.Limit limit = options.isLimited() ? io.lettuce.core.Limit.from(options.getCount())
				: io.lettuce.core.Limit.unlimited();

		XPendingArgs<byte[]> xPendingArgs = XPendingArgs.Builder.xpending(group, range, limit);
		if (options.hasConsumer()) {
			io.lettuce.core.Consumer<byte[]> consumer = io.lettuce.core.Consumer.from(group,
					LettuceConverters.toBytes(options.getConsumerName()));
			xPendingArgs.consumer(consumer);
		}
		if (options.hasIdle()) {
			xPendingArgs.idle(options.getIdle());
		}

		return connection.invoke().from(RedisStreamAsyncCommands::xpending, key, xPendingArgs)
				.get(it -> StreamConverters.toPendingMessages(groupName, options.getRange(), it));
	}

	@Override
	public List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range, Function.identity());
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);

		return connection.invoke().fromMany(RedisStreamAsyncCommands::xrange, key, lettuceRange, lettuceLimit)
				.toList(StreamConverters.byteRecordConverter());
	}

	@Override
	public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "StreamOffsets must not be null");

		XReadArgs.StreamOffset<byte[]>[] streamOffsets = toStreamOffsets(streams);
		XReadArgs args = StreamConverters.toReadArgs(readOptions);

		if (readOptions.isBlocking()) {

			return connection.invoke(getAsyncDedicatedConnection())
					.fromMany(RedisStreamAsyncCommands::xread, args, streamOffsets)
					.toList(StreamConverters.byteRecordConverter());
		}

		return connection.invoke().fromMany(RedisStreamAsyncCommands::xread, args, streamOffsets)
				.toList(StreamConverters.byteRecordConverter());
	}

	@Override
	public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {

		Assert.notNull(consumer, "Consumer must not be null");
		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "StreamOffsets must not be null");

		XReadArgs.StreamOffset<byte[]>[] streamOffsets = toStreamOffsets(streams);
		XReadArgs args = StreamConverters.toReadArgs(readOptions);
		io.lettuce.core.Consumer<byte[]> lettuceConsumer = toConsumer(consumer);

		if (readOptions.isBlocking()) {

			return connection.invoke(getAsyncDedicatedConnection())
					.fromMany(RedisStreamAsyncCommands::xreadgroup, lettuceConsumer, args, streamOffsets)
					.toList(StreamConverters.byteRecordConverter());
		}

		return connection.invoke().fromMany(RedisStreamAsyncCommands::xreadgroup, lettuceConsumer, args, streamOffsets)
				.toList(StreamConverters.byteRecordConverter());
	}

	@Override
	public List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range, Function.identity());
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);

		return connection.invoke()
				.fromMany(RedisStreamAsyncCommands::xrevrange, key, lettuceRange, lettuceLimit)
				.toList(StreamConverters.byteRecordConverter());
	}

	@Override
	public Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	public Long xTrim(byte[] key, long count, boolean approximateTrimming) {
		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStreamAsyncCommands::xtrim, key, approximateTrimming, count);
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncDedicatedConnection() {
		return connection.getAsyncDedicatedConnection();
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
