/*
 * Copyright 2021-2022 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.commands.StreamPipelineBinaryCommands;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamConsumersInfo;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.jedis.JedisInvoker.ResponseCommands;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.util.Assert;

/**
 * @author Dengliming
 * @since 2.3
 */
class JedisStreamCommands implements RedisStreamCommands {

	private final JedisConnection connection;

	JedisStreamCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		return connection.invoke().just(Jedis::xack, PipelineBinaryCommands::xack, key, JedisConverters.toBytes(group),
				StreamConverters.entryIdsToBytes(Arrays.asList(recordIds)));
	}

	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {

		Assert.notNull(record, "Record must not be null!");
		Assert.notNull(record.getStream(), "Stream must not be null!");

		XAddParams params = StreamConverters.toXAddParams(record.getId(), options);

		return connection.invoke()
				.from(Jedis::xadd, PipelineBinaryCommands::xadd, record.getStream(), record.getValue(), params)
				.get(it -> RecordId.of(JedisConverters.toString(it)));
	}

	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(group, "Group must not be null!");
		Assert.notNull(newOwner, "NewOwner must not be null!");

		XClaimParams params = StreamConverters.toXClaimParams(options);

		return connection.invoke()
				.fromMany(Jedis::xclaimJustId, ResponseCommands::xclaimJustId, key, JedisConverters.toBytes(group),
						JedisConverters.toBytes(newOwner), options.getMinIdleTime().toMillis(), params,
						StreamConverters.entryIdsToBytes(options.getIds()))
				.toList(it -> RecordId.of(JedisConverters.toString(it)));
	}

	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(group, "Group must not be null!");
		Assert.notNull(newOwner, "NewOwner must not be null!");

		XClaimParams params = StreamConverters.toXClaimParams(options);

		return connection.invoke()
				.from(Jedis::xclaim, ResponseCommands::xclaim, key, JedisConverters.toBytes(group),
						JedisConverters.toBytes(newOwner), options.getMinIdleTime().toMillis(), params,
						StreamConverters.entryIdsToBytes(options.getIds()))
				.get(r -> StreamConverters.convertToByteRecord(key, r));
	}

	@Override
	public Long xDel(byte[] key, RecordId... recordIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		return connection.invoke().just(Jedis::xdel, PipelineBinaryCommands::xdel, key,
				StreamConverters.entryIdsToBytes(Arrays.asList(recordIds)));
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
		return xGroupCreate(key, groupName, readOffset, false);
	}

	@Override
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");

		return connection.invoke().just(Jedis::xgroupCreate, PipelineBinaryCommands::xgroupCreate, key,
				JedisConverters.toBytes(groupName), JedisConverters.toBytes(readOffset.getOffset()), mkStream);
	}

	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(consumer, "Consumer must not be null!");

		return connection.invoke().from(Jedis::xgroupDelConsumer, PipelineBinaryCommands::xgroupDelConsumer, key,
				JedisConverters.toBytes(consumer.getGroup()), JedisConverters.toBytes(consumer.getName())).get(r -> r > 0);
	}

	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");

		return connection.invoke()
				.from(Jedis::xgroupDestroy, PipelineBinaryCommands::xgroupDestroy, key, JedisConverters.toBytes(groupName))
				.get(r -> r > 0);
	}

	@Override
	public StreamInfo.XInfoStream xInfo(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(Jedis::xinfoStream, ResponseCommands::xinfoStream, key).get(it -> {
			redis.clients.jedis.resps.StreamInfo streamInfo = BuilderFactory.STREAM_INFO.build(it);
			return StreamInfo.XInfoStream.fromList(StreamConverters.mapToList(streamInfo.getStreamInfo()));
		});
	}

	@Override
	public StreamInfo.XInfoGroups xInfoGroups(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(Jedis::xinfoGroups, StreamPipelineBinaryCommands::xinfoGroups, key).get(it -> {
			List<StreamGroupInfo> streamGroupInfos = BuilderFactory.STREAM_GROUP_INFO_LIST.build(it);
			List<Object> sources = new ArrayList<>();
			streamGroupInfos
					.forEach(streamGroupInfo -> sources.add(StreamConverters.mapToList(streamGroupInfo.getGroupInfo())));
			return StreamInfo.XInfoGroups.fromList(sources);
		});
	}

	@Override
	public StreamInfo.XInfoConsumers xInfoConsumers(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");

		return connection.invoke()
				.from(Jedis::xinfoConsumers, ResponseCommands::xinfoConsumers, key, JedisConverters.toBytes(groupName))
				.get(it -> {
					List<StreamConsumersInfo> streamConsumersInfos = BuilderFactory.STREAM_CONSUMERS_INFO_LIST.build(it);
					List<Object> sources = new ArrayList<>();
					streamConsumersInfos.forEach(
							streamConsumersInfo -> sources.add(StreamConverters.mapToList(streamConsumersInfo.getConsumerInfo())));
					return StreamInfo.XInfoConsumers.fromList(groupName, sources);
				});
	}

	@Override
	public Long xLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::xlen, PipelineBinaryCommands::xlen, key);
	}

	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.from(Jedis::xpending, PipelineBinaryCommands::xpending, key, JedisConverters.toBytes(groupName))
				.get(it -> StreamConverters.toPendingMessagesSummary(groupName, it));
	}

	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(groupName, "GroupName must not be null!");

		Range<String> range = (Range<String>) options.getRange();
		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		return connection.invoke()
				.from(Jedis::xpending, ResponseCommands::xpending, key, JedisConverters.toBytes(groupName), xPendingParams)
				.get(r -> StreamConverters.toPendingMessages(groupName, range,
						BuilderFactory.STREAM_PENDING_ENTRY_LIST.build(r)));
	}

	@Override
	public List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();

		return connection.invoke()
				.from(Jedis::xrange, ResponseCommands::xrange, key,
						JedisConverters.toBytes(StreamConverters.getLowerValue(range)),
						JedisConverters.toBytes(StreamConverters.getUpperValue(range)), count)
				.get(r -> StreamConverters.convertToByteRecord(key, r));
	}

	@Override
	public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		XReadParams params = StreamConverters.toXReadParams(readOptions);

		return connection.invoke()
				.from(Jedis::xread, ResponseCommands::xread, params, StreamConverters.toStreamOffsets(streams))
				.getOrElse(StreamConverters::convertToByteRecords, Collections::emptyList);
	}

	@Override
	public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
			StreamOffset<byte[]>... streams) {

		Assert.notNull(consumer, "Consumer must not be null!");
		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		XReadGroupParams params = StreamConverters.toXReadGroupParams(readOptions);

		return connection.invoke()
				.from(Jedis::xreadGroup, ResponseCommands::xreadGroup, JedisConverters.toBytes(consumer.getGroup()),
						JedisConverters.toBytes(consumer.getName()), params, StreamConverters.toStreamOffsets(streams))
				.getOrElse(StreamConverters::convertToByteRecords, Collections::emptyList);
	}

	@Override
	public List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();
		return connection.invoke()
				.from(Jedis::xrevrange, ResponseCommands::xrevrange, key,
						JedisConverters.toBytes(StreamConverters.getUpperValue(range)),
						JedisConverters.toBytes(StreamConverters.getLowerValue(range)), count)
				.get(it -> StreamConverters.convertToByteRecord(key, it));
	}

	@Override
	public Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	public Long xTrim(byte[] key, long count, boolean approximateTrimming) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(Jedis::xtrim, PipelineBinaryCommands::xtrim, key, count, approximateTrimming);
	}

}
