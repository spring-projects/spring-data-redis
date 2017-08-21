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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnection.LettuceResult;
import org.springframework.data.redis.connection.lettuce.LettuceConnection.LettuceTxResult;
import org.springframework.data.redis.core.types.Expiration;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceStringCommands implements RedisStringCommands {

	private final LettuceConnection connection;

	LettuceStringCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	public byte[] get(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().get(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().get(key)));
				return null;
			}
			return getConnection().get(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void set(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().set(key, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().set(key, value)));
				return;
			}
			getConnection().set(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[], org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOption)
	 */
	@Override
	public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(
						getAsyncConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option))));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(
						getConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option))));
				return;
			}
			getConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().getset(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().getset(key, value)));
				return null;
			}
			return getConnection().getset(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long append(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().append(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().append(key, value)));
				return null;
			}
			return getConnection().append(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> mGet(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().mget(keys), LettuceConverters.keyValueListUnwrapper()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceTxResult(getConnection().mget(keys), LettuceConverters.keyValueListUnwrapper()));
				return null;
			}

			return LettuceConverters.<byte[], byte[]> keyValueListUnwrapper().convert(getConnection().mget(keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().mset(tuples)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().mset(tuples)));
				return;
			}
			getConnection().mset(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().msetnx(tuples)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().msetnx(tuples)));
				return null;
			}
			return getConnection().msetnx(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().setex(key, time, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().setex(key, time, value)));
				return;
			}
			getConnection().setex(key, time, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/**
	 * @since 1.3
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public void pSetEx(byte[] key, long milliseconds, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().psetex(key, milliseconds, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().psetex(key, milliseconds, value)));
				return;
			}
			getConnection().psetex(key, milliseconds, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().setnx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().setnx(key, value)));
				return null;
			}
			return getConnection().setnx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] getRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().getrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().getrange(key, start, end)));
				return null;
			}
			return getConnection().getrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long decr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().decr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().decr(key)));
				return null;
			}
			return getConnection().decr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long decrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().decrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().decrby(key, value)));
				return null;
			}
			return getConnection().decrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long incr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().incr(key)));
				return null;
			}
			return getConnection().incr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long incrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().incrby(key, value)));
				return null;
			}
			return getConnection().incrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double incrBy(byte[] key, double value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incrbyfloat(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().incrbyfloat(key, value)));
				return null;
			}
			return getConnection().incrbyfloat(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceTxResult(getConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			return LettuceConverters.toBoolean(getConnection().getbit(key, offset));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().setbit(key, offset, LettuceConverters.toInt(value)),
						LettuceConverters.longToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().setbit(key, offset, LettuceConverters.toInt(value)),
						LettuceConverters.longToBooleanConverter()));
				return null;
			}
			return LettuceConverters.longToBooleanConverter()
					.convert(getConnection().setbit(key, offset, LettuceConverters.toInt(value)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().setrange(key, start, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxStatusResult(getConnection().setrange(key, start, value)));
				return;
			}
			getConnection().setrange(key, start, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().strlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().strlen(key)));
				return null;
			}
			return getConnection().strlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long bitCount(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().bitcount(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().bitcount(key)));
				return null;
			}
			return getConnection().bitcount(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long bitCount(byte[] key, long begin, long end) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().bitcount(key, begin, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(getConnection().bitcount(key, begin, end)));
				return null;
			}
			return getConnection().bitcount(key, begin, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(asyncBitOp(op, destination, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceTxResult(syncBitOp(op, destination, keys)));
				return null;
			}
			return syncBitOp(op, destination, keys);
		} catch (UnsupportedOperationException ex) {
			throw ex;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private Future<Long> asyncBitOp(BitOperation op, byte[] destination, byte[]... keys) {
		switch (op) {
			case AND:
				return getAsyncConnection().bitopAnd(destination, keys);
			case OR:
				return getAsyncConnection().bitopOr(destination, keys);
			case XOR:
				return getAsyncConnection().bitopXor(destination, keys);
			case NOT:
				if (keys.length != 1) {
					throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
				}
				return getAsyncConnection().bitopNot(destination, keys[0]);
			default:
				throw new UnsupportedOperationException("Bit operation " + op + " is not supported");
		}
	}

	private Long syncBitOp(BitOperation op, byte[] destination, byte[]... keys) {
		switch (op) {
			case AND:
				return getConnection().bitopAnd(destination, keys);
			case OR:
				return getConnection().bitopOr(destination, keys);
			case XOR:
				return getConnection().bitopXor(destination, keys);
			case NOT:
				if (keys.length != 1) {
					throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
				}
				return getConnection().bitopNot(destination, keys[0]);
			default:
				throw new UnsupportedOperationException("Bit operation " + op + " is not supported");
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

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}
}
