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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.ScanParams;

import java.util.List;
import java.util.Set;

import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisSetCommands implements RedisSetCommands {

	private final JedisConnection connection;

	public JedisSetCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sAdd(byte[], byte[][])
	 */
	@Override
	public Long sAdd(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sadd(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sadd(key, values)));
				return null;
			}
			return connection.getJedis().sadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sCard(byte[])
	 */
	@Override
	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().scard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().scard(key)));
				return null;
			}
			return connection.getJedis().scard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sdiff(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sdiff(keys)));
				return null;
			}
			return connection.getJedis().sdiff(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sdiffstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sdiffstore(destKey, keys)));
				return null;
			}
			return connection.getJedis().sdiffstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sinter(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sinter(keys)));
				return null;
			}
			return connection.getJedis().sinter(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sinterstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sinterstore(destKey, keys)));
				return null;
			}
			return connection.getJedis().sinterstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
	 */
	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sismember(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sismember(key, value)));
				return null;
			}
			return connection.getJedis().sismember(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMembers(byte[])
	 */
	@Override
	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().smembers(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().smembers(key)));
				return null;
			}
			return connection.getJedis().smembers(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().smove(srcKey, destKey, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().smove(srcKey, destKey, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().smove(srcKey, destKey, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
	 */
	@Override
	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().spop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().spop(key)));
				return null;
			}
			return connection.getJedis().spop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
	 */
	@Override
	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().srandmember(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().srandmember(key)));
				return null;
			}
			return connection.getJedis().srandmember(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[], long)
	 */
	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {

		if (count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count must be less than Integer.MAX_VALUE for sRandMember in Jedis.");
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().srandmember(key, (int) count)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().srandmember(key, (int) count)));
				return null;
			}
			return connection.getJedis().srandmember(key, (int) count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRem(byte[], byte[][])
	 */
	@Override
	public Long sRem(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().srem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().srem(key, values)));
				return null;
			}
			return connection.getJedis().srem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sunion(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sunion(keys)));
				return null;
			}
			return connection.getJedis().sunion(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().sunionstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().sunionstore(destKey, keys)));
				return null;
			}
			return connection.getJedis().sunionstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return sScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> sScan(byte[] key, long cursorId, ScanOptions options) {

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				redis.clients.jedis.ScanResult<byte[]> result = connection.getJedis().sscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(Long.valueOf(result.getStringCursor()), result.getResult());
			}

			protected void doClose() {
				JedisSetCommands.this.connection.close();
			};
		}.open();
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
