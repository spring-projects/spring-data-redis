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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
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
	public Long xAck(byte[] key, String group, String... messageIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");
		Assert.notNull(messageIds, "MessageIds must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().xack(key, LettuceConverters.toBytes(group), messageIds)));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().xack(key, LettuceConverters.toBytes(group), messageIds)));
				return null;
			}
			return getConnection().xack(key, LettuceConverters.toBytes(group), messageIds);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xAdd(byte[], java.util.Map)
	 */
	@Override
	public String xAdd(byte[] key, Map<byte[], byte[]> body) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(body, "Message body must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xadd(key, body)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xadd(key, body)));
				return null;
			}
			return getConnection().xadd(key, body);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xDel(byte[], java.lang.String[])
	 */
	@Override
	public Long xDel(byte[] key, String... messageIds) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(messageIds, "MessageIds must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xdel(key, messageIds)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xdel(key, messageIds)));
				return null;
			}
			return getConnection().xdel(key, messageIds);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xGroupCreate(byte[], org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Override
	public String xGroupCreate(byte[] key, ReadOffset readOffset, String group) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");
		Assert.notNull(readOffset, "ReadOffset must not be null!");

		try {
			XReadArgs.StreamOffset<byte[]> streamOffset = XReadArgs.StreamOffset.from(key, readOffset.getOffset());

			if (isPipelined()) {
				pipeline(connection
						.newLettuceResult(getAsyncConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(group))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newLettuceResult(getAsyncConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(group))));
				return null;
			}
			return getConnection().xgroupCreate(streamOffset, LettuceConverters.toBytes(group));
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
	public Boolean xGroupDestroy(byte[] key, String group) {

		Assert.notNull(key, "Key must not be null!");
		Assert.hasText(group, "Group name must not be null or empty!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().xgroupDestroy(key, LettuceConverters.toBytes(group))));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().xgroupDestroy(key, LettuceConverters.toBytes(group))));
				return null;
			}
			return getConnection().xgroupDestroy(key, LettuceConverters.toBytes(group));
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
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<StreamMessage<byte[], byte[]>> xRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range, Function.identity());
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xrange(key, lettuceRange, lettuceLimit),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xrange(key, lettuceRange, lettuceLimit),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			return StreamConverters.toStreamMessages(getConnection().xrange(key, lettuceRange, lettuceLimit));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRead(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StreamMessage<byte[], byte[]>> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null!");
		Assert.notNull(streams, "StreamOffsets must not be null!");

		XReadArgs.StreamOffset<byte[]>[] streamOffsets = toStreamOffsets(streams);
		XReadArgs args = StreamConverters.toReadArgs(readOptions);

		if (isBlocking(readOptions)) {

			try {
				if (isPipelined()) {
					pipeline(connection.newLettuceResult(getAsyncDedicatedConnection().xread(args, streamOffsets),
							StreamConverters.streamMessageListConverter()));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(getAsyncDedicatedConnection().xread(args, streamOffsets),
							StreamConverters.streamMessageListConverter()));
					return null;
				}
				return StreamConverters.toStreamMessages(getDedicatedConnection().xread(args, streamOffsets));
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xread(args, streamOffsets),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xread(args, streamOffsets),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			return StreamConverters.toStreamMessages(getConnection().xread(args, streamOffsets));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xReadGroup(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset[])
	 */
	@Override
	public List<StreamMessage<byte[], byte[]>> xReadGroup(Consumer consumer, StreamReadOptions readOptions,
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
							StreamConverters.streamMessageListConverter()));
					return null;
				}
				if (isQueueing()) {
					transaction(connection.newLettuceResult(
							getAsyncDedicatedConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
							StreamConverters.streamMessageListConverter()));
					return null;
				}
				return StreamConverters
						.toStreamMessages(getDedicatedConnection().xreadgroup(lettuceConsumer, args, streamOffsets));
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xreadgroup(lettuceConsumer, args, streamOffsets),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			return StreamConverters.toStreamMessages(getConnection().xreadgroup(lettuceConsumer, args, streamOffsets));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStreamCommands#xRevRange(byte[], org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public List<StreamMessage<byte[], byte[]>> xRevRange(byte[] key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(range);
		io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(limit);
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().xrevrange(key, lettuceRange, lettuceLimit),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().xrevrange(key, lettuceRange, lettuceLimit),
						StreamConverters.streamMessageListConverter()));
				return null;
			}
			return StreamConverters.toStreamMessages(getConnection().xrevrange(key, lettuceRange, lettuceLimit));
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
}
