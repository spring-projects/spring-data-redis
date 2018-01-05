/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.Protocol;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisListCommands implements RedisListCommands {

	private final @NonNull JedisConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPush(byte[], byte[][])
	 */
	@Override
	public Long rPush(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().rpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().rpush(key, values)));
				return null;
			}
			return connection.getJedis().rpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lpush(key, values)));
				return null;
			}
			return connection.getJedis().lpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().rpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().rpushx(key, value)));
				return null;
			}
			return connection.getJedis().rpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lpushx(key, value)));
				return null;
			}
			return connection.getJedis().lpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().llen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().llen(key)));
				return null;
			}
			return connection.getJedis().llen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lrange(key, start, end)));
				return null;
			}
			return connection.getJedis().lrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newStatusResult(connection.getRequiredPipeline().ltrim(key, start, end)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getRequiredTransaction().ltrim(key, start, end)));
				return;
			}
			connection.getJedis().ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lindex(key, index)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lindex(key, index)));
				return null;
			}
			return connection.getJedis().lindex(key, index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(
						connection.getRequiredPipeline().linsert(key, JedisConverters.toListPosition(where), pivot, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(
						connection.getRequiredTransaction().linsert(key, JedisConverters.toListPosition(where), pivot, value)));
				return null;
			}
			return connection.getJedis().linsert(key, JedisConverters.toListPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newStatusResult(connection.getRequiredPipeline().lset(key, index, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getRequiredTransaction().lset(key, index, value)));
				return;
			}
			connection.getJedis().lset(key, index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lrem(key, count, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lrem(key, count, value)));
				return null;
			}
			return connection.getJedis().lrem(key, count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().lpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().lpop(key)));
				return null;
			}
			return connection.getJedis().lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().rpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().rpop(key)));
				return null;
			}
			return connection.getJedis().rpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().blpop(bXPopArgs(timeout, keys))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().blpop(bXPopArgs(timeout, keys))));
				return null;
			}
			return connection.getJedis().blpop(timeout, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().brpop(bXPopArgs(timeout, keys))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().brpop(bXPopArgs(timeout, keys))));
				return null;
			}
			return connection.getJedis().brpop(timeout, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().rpoplpush(srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().rpoplpush(srcKey, dstKey)));
				return null;
			}
			return connection.getJedis().rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().brpoplpush(srcKey, dstKey, timeout)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().brpoplpush(srcKey, dstKey, timeout)));
				return null;
			}
			return connection.getJedis().brpoplpush(srcKey, dstKey, timeout);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private byte[][] bXPopArgs(int timeout, byte[]... keys) {

		List<byte[]> args = new ArrayList<>();
		for (byte[] arg : keys) {
			args.add(arg);
		}
		args.add(Protocol.toByteArray(timeout));
		return args.toArray(new byte[args.size()][]);
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
}
