/*
 * Copyright 2017 the original author or authors.
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

import redis.clients.jedis.Protocol;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisListCommands implements RedisListCommands {

	private final JedisConnection connection;

	public JedisListCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPush(byte[], byte[][])
	 */
	@Override
	public Long rPush(byte[] key, byte[]... values) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().rpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().rpush(key, values)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lpush(key, values)));
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
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().rpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().rpushx(key, value)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lpushx(key, value)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().llen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().llen(key)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lrange(key, start, end)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().ltrim(key, start, end)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().ltrim(key, start, end)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lindex(key, index)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lindex(key, index)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(
						connection.getPipeline().linsert(key, JedisConverters.toListPosition(where), pivot, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(
						connection.getTransaction().linsert(key, JedisConverters.toListPosition(where), pivot, value)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().lset(key, index, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().lset(key, index, value)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lrem(key, count, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lrem(key, count, value)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().lpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().lpop(key)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().rpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().rpop(key)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().blpop(bXPopArgs(timeout, keys))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().blpop(bXPopArgs(timeout, keys))));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().brpop(bXPopArgs(timeout, keys))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().brpop(bXPopArgs(timeout, keys))));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().rpoplpush(srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().rpoplpush(srcKey, dstKey)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().brpoplpush(srcKey, dstKey, timeout)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().brpoplpush(srcKey, dstKey, timeout)));
				return null;
			}
			return connection.getJedis().brpoplpush(srcKey, dstKey, timeout);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private byte[][] bXPopArgs(int timeout, byte[]... keys) {

		final List<byte[]> args = new ArrayList<>();
		for (final byte[] arg : keys) {
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
