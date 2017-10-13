/*
 * Copyright 2017 the original author or authors.
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

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceTxResult;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class LettuceListCommands implements RedisListCommands {

	private final @NonNull LettuceConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPush(byte[], byte[][])
	 */
	@Override
	public Long rPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().rpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().rpush(key, values)));
				return null;
			}
			return getConnection().rpush(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPush(byte[], byte[][])
	 */
	@Override
	public Long lPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lpush(key, values)));
				return null;
			}
			return getConnection().lpush(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPushX(byte[], byte[])
	 */
	@Override
	public Long rPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().rpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().rpushx(key, value)));
				return null;
			}
			return getConnection().rpushx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPushX(byte[], byte[])
	 */
	@Override
	public Long lPushX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lpushx(key, value)));
				return null;
			}
			return getConnection().lpushx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lLen(byte[])
	 */
	@Override
	public Long lLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().llen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().llen(key)));
				return null;
			}
			return getConnection().llen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRange(byte[], long, long)
	 */
	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lrange(key, start, end)));
				return null;
			}
			return getConnection().lrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lTrim(byte[], long, long)
	 */
	@Override
	public void lTrim(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().ltrim(key, start, end)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().ltrim(key, start, end)));
				return;
			}
			getConnection().ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lIndex(byte[], long)
	 */
	@Override
	public byte[] lIndex(byte[] key, long index) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lindex(key, index)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lindex(key, index)));
				return null;
			}
			return getConnection().lindex(key, index);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lInsert(byte[], org.springframework.data.redis.connection.RedisListCommands.Position, byte[], byte[])
	 */
	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection
						.newLettuceResult(getAsyncConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newLettuceTxResult(getConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value)));
				return null;
			}
			return getConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lSet(byte[], long, byte[])
	 */
	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().lset(key, index, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().lset(key, index, value)));
				return;
			}
			getConnection().lset(key, index, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRem(byte[], long, byte[])
	 */
	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lrem(key, count, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lrem(key, count, value)));
				return null;
			}
			return getConnection().lrem(key, count, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPop(byte[])
	 */
	@Override
	public byte[] lPop(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().lpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().lpop(key)));
				return null;
			}
			return getConnection().lpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPop(byte[])
	 */
	@Override
	public byte[] rPop(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().rpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().rpop(key)));
				return null;
			}
			return getConnection().rpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bLPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(connection.getAsyncDedicatedConnection().blpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(connection.getDedicatedConnection().blpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			return LettuceConverters.toBytesList(connection.getDedicatedConnection().blpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {

		Assert.notNull(keys, "Key must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(connection.getAsyncDedicatedConnection().brpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(connection.getDedicatedConnection().brpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			return LettuceConverters.toBytesList(connection.getDedicatedConnection().brpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPopLPush(byte[], byte[])
	 */
	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(dstKey, "Destination key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().rpoplpush(srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().rpoplpush(srcKey, dstKey)));
				return null;
			}
			return getConnection().rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPopLPush(int, byte[], byte[])
	 */
	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(dstKey, "Destination key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(connection.getAsyncDedicatedConnection().brpoplpush(timeout, srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceTxResult(connection.getDedicatedConnection().brpoplpush(timeout, srcKey, dstKey)));
				return null;
			}
			return connection.getDedicatedConnection().brpoplpush(timeout, srcKey, dstKey);
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

	private void transaction(LettuceTxResult result) {
		connection.transaction(result);
	}

	private RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}

}
