/*
 * Copyright 2021 the original author or authors.
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

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConverters;
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
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.MultiKeyPipelineBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.data.redis.connection.jedis.StreamConverters.convertToByteRecord;

/**
 * @author Dengliming
 * @since 2.3
 */
class JedisStreamCommands implements RedisStreamCommands {

	private final JedisConnection connection;

	JedisStreamCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAck(byte[], String, org.springframework.data.redis.connection.stream.RecordId[])
	 */
	@Override
	public Long xAck(byte[] key, String group, RecordId... recordIds) {
		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");
		Assert.notNull(recordIds, "recordIds must not be null!");

		return connection.invoke().just(BinaryJedis::xack, MultiKeyPipelineBase::xack, key, JedisConverters.toBytes(group),
				entryIdsToBytes(recordIds));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAdd(MapRecord, XAddOptions)
	 */
	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {
		Assert.notNull(record, "Record must not be null!");
		Assert.notNull(record.getStream(), "Stream must not be null!");

		byte[] id = JedisConverters.toBytes(record.getId().getValue());
		Long maxLength = Long.MAX_VALUE;
		if (options.hasMaxlen()) {
			maxLength = options.getMaxlen();
		}

		return connection.invoke().from(BinaryJedis::xadd, MultiKeyPipelineBase::xadd, record.getStream(), id,
				record.getValue(), maxLength, false).get(it -> RecordId.of(JedisConverters.toString(it)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaimJustId(byte[], java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
		throw new UnsupportedOperationException("Jedis does not support xClaimJustId.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaim(byte[], java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(group, "Group must not be null!");
		Assert.notNull(newOwner, "NewOwner must not be null!");

		long minIdleTime = -1L;
		if (options.getMinIdleTime() != null) {
			minIdleTime = options.getMinIdleTime().toMillis();
		}
		int retryCount = -1;
		if (options.getRetryCount() != null) {
			retryCount = options.getRetryCount().intValue();
		}
		long unixTime = -1L;
		if (options.getUnixTime() != null) {
			unixTime = options.getUnixTime().toEpochMilli();
		}
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().xclaim(key, JedisConverters.toBytes(group),
						JedisConverters.toBytes(newOwner), minIdleTime, unixTime, retryCount, options.isForce(),
						entryIdsToBytes(options.getIds()))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().xclaim(key,
						JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner), minIdleTime, unixTime, retryCount,
						options.isForce(), entryIdsToBytes(options.getIds()))));
				return null;
			}

			return convertToByteRecord(key,
					connection.getJedis().xclaim(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
							minIdleTime, unixTime, retryCount, options.isForce(), entryIdsToBytes(options.getIds())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		return connection.invoke().just(BinaryJedis::xdel, MultiKeyPipelineBase::xdel, key, entryIdsToBytes(recordIds));
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
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {
		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");

		return connection.invoke().just(BinaryJedis::xgroupCreate, MultiKeyPipelineBase::xgroupCreate, key,
				JedisConverters.toBytes(groupName), JedisConverters.toBytes(readOffset.getOffset()), mkStream);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDelConsumer(byte[], org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Override
	public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(consumer, "Consumer must not be null!");

		return connection.invoke().from(BinaryJedis::xgroupDelConsumer, MultiKeyPipelineBase::xgroupDelConsumer, key,
				JedisConverters.toBytes(consumer.getGroup()), JedisConverters.toBytes(consumer.getName())).get(r -> r > 0);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupDestroy(byte[], java.lang.String)
	 */
	@Override
	public Boolean xGroupDestroy(byte[] key, String groupName) {
		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");

		return connection.invoke()
				.from(BinaryJedis::xgroupDestroy, MultiKeyPipelineBase::xgroupDestroy, key, JedisConverters.toBytes(groupName))
				.get(r -> r > 0);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfo(byte[])
	 */
	@Override
	public StreamInfo.XInfoStream xInfo(byte[] key) {
		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(BinaryJedis::xinfoStream, (k, r) -> {
			throw new UnsupportedOperationException("'XINFO' cannot be called in pipeline / transaction mode.");
		}, key).get(streamInfo -> StreamInfo.XInfoStream.fromList(mapToList(streamInfo.getStreamInfo())));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoGroups(byte[])
	 */
	@Override
	public StreamInfo.XInfoGroups xInfoGroups(byte[] key) {
		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(BinaryJedis::xinfoGroup, (k, r) -> {
			throw new UnsupportedOperationException("'XINFO GROUPS' cannot be called in pipeline / transaction mode.");
		}, key).get(streamGroupInfos -> {
			List<Object> sources = new ArrayList<>();
			streamGroupInfos.forEach(streamGroupInfo -> sources.add(mapToList(streamGroupInfo.getGroupInfo())));
			return StreamInfo.XInfoGroups.fromList(sources);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoConsumers(byte[], java.lang.String)
	 */
	@Override
	public StreamInfo.XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");

		return connection.invoke().from(BinaryJedis::xinfoConsumers, (k, v, r) -> {
			throw new UnsupportedOperationException("'XINFO CONSUMERS' cannot be called in pipeline / transaction mode.");
		}, key, JedisConverters.toBytes(groupName)).get(streamConsumersInfos -> {
			List<Object> sources = new ArrayList<>();
			streamConsumersInfos
					.forEach(streamConsumersInfo -> sources.add(mapToList(streamConsumersInfo.getConsumerInfo())));
			return StreamInfo.XInfoConsumers.fromList(groupName, sources);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xLen(byte[])
	 */
	@Override
	public Long xLen(byte[] key) {
		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::xlen, MultiKeyPipelineBase::xlen, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String)
	 */
	@Override
	public PendingMessagesSummary xPending(byte[] key, String groupName) {
		throw new UnsupportedOperationException("Jedis does not support returning PendingMessagesSummary.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xPending(byte[], java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions)
	 */
	@Override
	public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(groupName, "GroupName must not be null!");

		Range<String> range = (Range<String>) options.getRange();
		byte[] group = LettuceConverters.toBytes(groupName);
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().xpending(key, group,
						JedisConverters.toBytes(getLowerValue(range)), JedisConverters.toBytes(getUpperValue(range)),
						options.getCount().intValue(), JedisConverters.toBytes(options.getConsumerName()))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().xpending(key, group,
						JedisConverters.toBytes(getLowerValue(range)), JedisConverters.toBytes(getUpperValue(range)),
						options.getCount().intValue(), JedisConverters.toBytes(options.getConsumerName()))));
				return null;
			}

			return StreamConverters.toPendingMessages(groupName, range,
					connection.getJedis().xpending(key, group, JedisConverters.toBytes(getLowerValue(range)),
							JedisConverters.toBytes(getUpperValue(range)), options.getCount().intValue(),
							JedisConverters.toBytes(options.getConsumerName())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRange(byte[] key, Range<String> range, RedisZSetCommands.Limit limit) {
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().xrange(key,
						JedisConverters.toBytes(range.getLowerBound().getValue().get()),
						JedisConverters.toBytes(range.getUpperBound().getValue().get()), count)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().xrange(key,
						JedisConverters.toBytes(range.getLowerBound().getValue().get()),
						JedisConverters.toBytes(range.getUpperBound().getValue().get()), count)));
				return null;
			}

			return convertToByteRecord(key, connection.getJedis().xrange(key, JedisConverters.toBytes(getLowerValue(range)),
					JedisConverters.toBytes(getUpperValue(range)), count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		Long block = readOptions.getBlock();
		if (block == null) {
			block = -1L;
		}
		int count = Integer.MAX_VALUE;
		if (readOptions.getCount() != null) {
			count = readOptions.getCount().intValue();
		}
		return connection.invoke().from(BinaryJedis::xread, (t1, t2, t3, r) -> {
			throw new UnsupportedOperationException("Operation not supported in pipelining/transaction mode");
		}, count, block, toStreamOffsets(streams)).get(streamsEntries -> convertToByteRecord(streamsEntries));
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

		long block = -1L;
		if (readOptions.getBlock() != null) {
			block = readOptions.getBlock();
		}
		int count = -1;
		if (readOptions.getCount() != null) {
			count = readOptions.getCount().intValue();
		}
		return connection.invoke().from(BinaryJedis::xreadGroup, (t1, t2, t3, t4, t5, t6, r) -> {
			throw new UnsupportedOperationException("Operation not supported in pipelining/transaction mode");
		}, JedisConverters.toBytes(consumer.getGroup()), JedisConverters.toBytes(consumer.getName()), count, block,
				readOptions.isNoack(), toStreamOffsets(streams)).get(streamsEntries -> convertToByteRecord(streamsEntries));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRevRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<ByteRecord> xRevRange(byte[] key, Range<String> range, RedisZSetCommands.Limit limit) {
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();
		return connection.invoke()
				.from(BinaryJedis::xrevrange, MultiKeyPipelineBase::xrevrange, key,
						JedisConverters.toBytes(getUpperValue(range)), JedisConverters.toBytes(getLowerValue(range)), count)
				.get(it -> convertToByteRecord(key, it));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xTrim(byte[], long)
	 */
	@Override
	public Long xTrim(byte[] key, long count) {
		return xTrim(key, count, false);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xTrim(byte[], long, boolean)
	 */
	@Override
	public Long xTrim(byte[] key, long count, boolean approximateTrimming) {
		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::xtrim, MultiKeyPipelineBase::xtrim, key, count, approximateTrimming);
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private void pipeline(JedisResult result) {
		connection.pipeline(result);
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void transaction(JedisResult result) {
		connection.transaction(result);
	}

	private RuntimeException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

	private byte[][] entryIdsToBytes(RecordId[] recordIds) {

		final byte[][] bids = new byte[recordIds.length][];
		for (int i = 0; i < recordIds.length; ++i) {
			RecordId id = recordIds[i];
			bids[i] = JedisConverters.toBytes(id.getValue());
		}

		return bids;
	}

	private byte[][] entryIdsToBytes(List<RecordId> recordIds) {

		final byte[][] bids = new byte[recordIds.size()][];
		for (int i = 0; i < recordIds.size(); ++i) {
			RecordId id = recordIds.get(i);
			bids[i] = JedisConverters.toBytes(id.getValue());
		}

		return bids;
	}

	private String getLowerValue(Range<String> range) {

		if (range.getLowerBound().equals(Range.Bound.unbounded())) {
			return "-";
		}

		return range.getLowerBound().getValue().orElse("-");
	}

	private String getUpperValue(Range<String> range) {

		if (range.getUpperBound().equals(Range.Bound.unbounded())) {
			return "+";
		}

		return range.getUpperBound().getValue().orElse("+");
	}

	private List<Object> mapToList(Map<String, Object> map) {
		List<Object> sources = new ArrayList<>(map.size() * 2);
		map.forEach((k, v) -> {
			sources.add(k);
			sources.add(v);
		});
		return sources;
	}

	private Map<byte[], byte[]> toStreamOffsets(StreamOffset<byte[]>... streams) {
		return Arrays.stream(streams)
				.collect(Collectors.toMap(k -> k.getKey(), v -> JedisConverters.toBytes(v.getOffset().getOffset())));
	}
}
