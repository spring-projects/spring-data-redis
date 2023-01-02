/*
 * Copyright 2021-2023 the original author or authors.
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

import static org.springframework.data.redis.connection.jedis.StreamConverters.*;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.params.XAddParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
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
class JedisClusterStreamCommands implements RedisStreamCommands {

	private final JedisClusterConnection connection;

	JedisClusterStreamCommands(JedisClusterConnection connection) {
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

		try {
			return connection.getCluster().xack(key, JedisConverters.toBytes(group),
					entryIdsToBytes(Arrays.asList(recordIds)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAdd(MapRecord, XAddOptions)
	 */
	@Override
	public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {

		Assert.notNull(record, "Record must not be null!");
		Assert.notNull(record.getStream(), "Stream must not be null!");

		XAddParams params = StreamConverters.toXAddParams(record, options);

		try {
			return RecordId
					.of(JedisConverters.toString(connection.getCluster().xadd(record.getStream(), record.getValue(), params)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xClaimJustId(byte[], java.lang.String, java.lang.String, org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions)
	 */
	@Override
	public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
		throw new UnsupportedOperationException("JedisCluster does not support xClaimJustId.");
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

		long minIdleTime = options.getMinIdleTime() == null ? -1L : options.getMinIdleTime().toMillis();
		int retryCount = options.getRetryCount() == null ? -1 : options.getRetryCount().intValue();
		long unixTime = options.getUnixTime() == null ? -1L : options.getUnixTime().toEpochMilli();

		try {
			return convertToByteRecord(key,
					connection.getCluster().xclaim(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
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

		try {
			return connection.getCluster().xdel(key, entryIdsToBytes(Arrays.asList(recordIds)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
	public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean mkStream) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(groupName, "Group name must not be null or empty!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");

		try {
			return connection.getCluster().xgroupCreate(key, JedisConverters.toBytes(groupName),
					JedisConverters.toBytes(readOffset.getOffset()), mkStream);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
			return connection.getCluster().xgroupDelConsumer(key, JedisConverters.toBytes(consumer.getGroup()),
					JedisConverters.toBytes(consumer.getName())) != 0L;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
			return connection.getCluster().xgroupDestroy(key, JedisConverters.toBytes(groupName)) != 0L;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfo(byte[])
	 */
	@Override
	public StreamInfo.XInfoStream xInfo(byte[] key) {
		throw new UnsupportedOperationException("JedisCluster does not support XINFO.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoGroups(byte[])
	 */
	@Override
	public StreamInfo.XInfoGroups xInfoGroups(byte[] key) {
		throw new UnsupportedOperationException("JedisCluster does not support XINFO.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xInfoConsumers(byte[], java.lang.String)
	 */
	@Override
	public StreamInfo.XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
		throw new UnsupportedOperationException("JedisCluster does not support XINFO.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xLen(byte[])
	 */
	@Override
	public Long xLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().xlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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
		byte[] group = JedisConverters.toBytes(groupName);

		try {

			List<Object> response = connection.getCluster().xpending(key, group,
					JedisConverters.toBytes(getLowerValue(range)), JedisConverters.toBytes(getUpperValue(range)),
					options.getCount().intValue(), JedisConverters.toBytes(options.getConsumerName()));

			return StreamConverters.toPendingMessages(groupName, range,
					BuilderFactory.STREAM_PENDING_ENTRY_LIST.build(response));
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
			return convertToByteRecord(key, connection.getCluster().xrange(key, JedisConverters.toBytes(getLowerValue(range)),
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

		long block = readOptions.getBlock() == null ? -1L : readOptions.getBlock();
		int count = readOptions.getCount() != null ? readOptions.getCount().intValue() : Integer.MAX_VALUE;

		try {

			List<byte[]> xread = connection.getCluster().xread(count, block, toStreamOffsets(streams));

			if (xread == null) {
				return Collections.emptyList();
			}

			return StreamConverters.convertToByteRecords(xread);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		long block = readOptions.getBlock() == null ? -1L : readOptions.getBlock();
		int count = readOptions.getCount() == null ? -1 : readOptions.getCount().intValue();

		try {

			List<byte[]> xread = connection.getCluster().xreadGroup(JedisConverters.toBytes(consumer.getGroup()),
					JedisConverters.toBytes(consumer.getName()), count, block, readOptions.isNoack(), toStreamOffsets(streams));

			if (xread == null) {
				return Collections.emptyList();
			}

			return StreamConverters.convertToByteRecords(xread);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			return convertToByteRecord(key, connection.getCluster().xrevrange(key,
					JedisConverters.toBytes(getUpperValue(range)), JedisConverters.toBytes(getLowerValue(range)), count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			return connection.getCluster().xtrim(key, count, approximateTrimming);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
