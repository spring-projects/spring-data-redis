/*
 * Copyright 2015 the original author or authors.
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

import static org.springframework.util.Assert.*;
import static org.springframework.util.ReflectionUtils.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisPool;

public class JedisClusterConnection implements RedisClusterConnection {

	// TODO: move this to common class since copied from JedisCOnnection
	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private static final Field CONNECTION_HANDLER;
	private final JedisCluster cluster;
	private final ThreadPoolTaskExecutor executor;

	static {

		Field connectionHandler = findField(JedisCluster.class, "connectionHandler");
		makeAccessible(connectionHandler);
		CONNECTION_HANDLER = connectionHandler;
	}

	public JedisClusterConnection(JedisCluster cluster) {

		notNull(cluster);
		this.cluster = cluster;

		executor = new ThreadPoolTaskExecutor();
		this.executor.initialize();
	}

	@Override
	public void close() throws DataAccessException {
		cluster.close();
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public Object getNativeConnection() {
		return cluster;
	}

	@Override
	public boolean isQueueing() {
		return false;
	}

	@Override
	public boolean isPipelined() {
		return false;
	}

	@Override
	public void openPipeline() {

	}

	@Override
	public List<Object> closePipeline() throws RedisPipelineException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object execute(String command, byte[]... args) {
		// return new JedisClusterCommand(this.connectionHandler, 1, 5){
		//
		// @Override
		// public Object execute(Jedis connection) {
		// jedis.e
		// }};
		return null;
	}

	@Override
	public Long del(byte[]... keys) {

		noNullElements(keys, "Keys must not be null or contain null key!");
		try {
			for (byte[] key : keys) {
				cluster.del(JedisConverters.toString(key));
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
		return null;
	}

	@Override
	public DataType type(byte[] key) {
		try {
			return JedisConverters.toDataType(cluster.type(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> keys(final byte[] pattern) {

		notNull(pattern, "Pattern must not be null!");

		Collection<Set<byte[]>> keysPerNode = runCommandOnAllNodes(new JedisCommandCallback<Set<byte[]>>() {

			@Override
			public Set<byte[]> doInJedis(Jedis jedis) {
				return jedis.keys(pattern);
			}
		}).values();

		Set<byte[]> keys = new HashSet<byte[]>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	@Override
	public Set<byte[]> keys(final byte[] pattern, RedisNode node) {

		notNull(pattern, "Pattern must not be null!");

		return runCommandOnSingleNode(new JedisCommandCallback<Set<byte[]>>() {

			@Override
			public Set<byte[]> doInJedis(Jedis jedis) {
				return jedis.keys(pattern);
			}
		}, node);
	}

	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] randomKey() {

		List<RedisNode> nodes = getClusterNodes();
		int iteration = 0;
		do {
			RedisNode node = nodes.get(new Random().nextInt(nodes.size()));
			byte[] key = randomKey(node);

			if (key != null && key.length > 0) {
				return key;
			}
			iteration++;
		} while (iteration < nodes.size());

		return null;
	}

	@Override
	public byte[] randomKey(RedisNode node) {

		return runCommandOnSingleNode(new JedisCommandCallback<byte[]>() {

			@Override
			public byte[] doInJedis(Jedis jedis) {
				return jedis.randomBinaryKey();
			}
		}, node);
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {

		if (seconds > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException();
		}
		try {
			return JedisConverters.toBoolean(cluster.expire(JedisConverters.toString(key), Long.valueOf(seconds).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpire(final byte[] key, final long millis) {

		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 1, 5) {
			@Override
			public Boolean execute(Jedis connection) {
				return JedisConverters.toBoolean(connection.pexpire(key, millis));
			}
		}, key);
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {

		try {
			return JedisConverters.toBoolean(cluster.expireAt(JedisConverters.toString(key), unixTime));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpireAt(final byte[] key, final long unixTimeInMillis) {

		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 1, 5) {

			@Override
			public Boolean execute(Jedis connection) {
				return JedisConverters.toBoolean(connection.pexpire(key, unixTimeInMillis));
			}
		}, key);
	}

	@Override
	public Boolean persist(byte[] key) {

		try {
			return JedisConverters.toBoolean(cluster.persist(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long ttl(byte[] key) {

		try {
			return cluster.ttl(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pTtl(final byte[] key) {

		return runClusterCommand(new JedisClusterCommand<Long>(getClusterConnectionHandler(), 5, 1) {

			@Override
			public Long execute(Jedis connection) {
				return connection.pttl(key);
			}
		}, key);
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		try {
			List<String> sorted = cluster.sort(JedisConverters.toString(key), JedisConverters.toSortingParams(params));
			return JedisConverters.stringListToByteList().convert(sorted);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		throw new UnsupportedOperationException("Maybe we could sort and then store using new key");
	}

	@Override
	public byte[] dump(final byte[] key) {

		return runClusterCommand(new JedisClusterCommand<byte[]>(getClusterConnectionHandler(), 5, 1) {

			@Override
			public byte[] execute(Jedis connection) {
				return connection.dump(key);
			}
		}, key);
	}

	@Override
	public void restore(final byte[] key, final long ttlInMillis, final byte[] serializedValue) {

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException();
		}

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 5, 1) {

			@Override
			public Void execute(Jedis connection) {

				connection.restore(key, Long.valueOf(ttlInMillis).intValue(), serializedValue);
				return null;
			}
		}, key);
	}

	@Override
	public byte[] get(byte[] key) {

		try {
			return JedisConverters.toBytes(cluster.get(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		try {
			return JedisConverters.toBytes(cluster.getSet(JedisConverters.toString(key), JedisConverters.toString(value)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {
		throw new UnsupportedOperationException("Move is not supported in cluster mode");
	}

	@Override
	public void set(byte[] key, byte[] value) {

		try {
			JedisConverters.toBytes(cluster.set(JedisConverters.toString(key), JedisConverters.toString(value)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.setnx(JedisConverters.toString(key), JedisConverters.toString(value)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Seconds have cannot exceed Integer.MAX_VALUE!");
		}

		try {
			cluster.setex(JedisConverters.toString(key), Long.valueOf(seconds).intValue(), JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSetEx(final byte[] key, final long milliseconds, final byte[] value) {

		if (milliseconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Milliseconds have cannot exceed Integer.MAX_VALUE!");
		}

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 1, 5) {

			@Override
			public Void execute(Jedis connection) {

				connection.psetex(key, Long.valueOf(milliseconds).intValue(), value);
				return null;
			}
		}, key);
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuple) {
		throw new UnsupportedOperationException(
				"Need to check this since it should be possibel if key prefix is correct. see http://redis.io/topics/cluster-spec#multiple-keys-operations");
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuple) {
		throw new UnsupportedOperationException(
				"Need to check this since it should be possibel if key prefix is correct. see http://redis.io/topics/cluster-spec#multiple-keys-operations");
	}

	@Override
	public Long incr(byte[] key) {

		try {
			return cluster.incr(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		try {
			return cluster.incrBy(JedisConverters.toString(key), value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double incrBy(final byte[] key, final double value) {

		return runClusterCommand(new JedisClusterCommand<Double>(getClusterConnectionHandler(), 5, 1) {

			@Override
			public Double execute(Jedis connection) {
				return connection.incrByFloat(key, value);
			}
		}, key);
	}

	@Override
	public Long decr(byte[] key) {

		try {
			return cluster.decr(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decrBy(byte[] key, long value) {

		try {
			return cluster.decrBy(JedisConverters.toString(key), value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		try {
			return cluster.append(JedisConverters.toString(key), JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte[] key, long begin, long end) {

		try {
			return JedisConverters.toBytes(cluster.getrange(JedisConverters.toString(key), begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		try {
			cluster.setrange(JedisConverters.toString(key), offset, JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		try {
			return cluster.getbit(JedisConverters.toString(key), offset);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		try {
			return cluster.setbit(JedisConverters.toString(key), offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key) {

		try {
			return cluster.bitcount(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key, long begin, long end) {

		try {
			return cluster.bitcount(JedisConverters.toString(key), begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		throw new UnsupportedOperationException("not allowed on multiple keys, but one on same host would be ok");
	}

	@Override
	public Long strLen(byte[] key) {

		try {
			return cluster.strlen(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPush(byte[] key, byte[]... values) {

		try {
			return cluster.rpush(JedisConverters.toString(key), JedisConverters.toStrings(values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPush(byte[] key, byte[]... values) {

		try {
			return cluster.lpush(JedisConverters.toString(key), JedisConverters.toStrings(values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {

		try {
			return cluster.rpushx(JedisConverters.toString(key), JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {

		try {
			return cluster.lpushx(JedisConverters.toString(key), JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lLen(byte[] key) {

		try {
			return cluster.llen(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> lRange(byte[] key, long begin, long end) {

		try {
			return JedisConverters.stringListToByteList().convert(cluster.lrange(JedisConverters.toString(key), begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lTrim(final byte[] key, final long begin, final long end) {

		runClusterCommand(new JedisClusterCommand<Void>(getClusterConnectionHandler(), 5, 1) {

			@Override
			public Void execute(Jedis connection) {

				connection.ltrim(key, begin, end);
				return null;
			}
		}, key);

	}

	@Override
	public byte[] lIndex(byte[] key, long index) {

		try {
			return JedisConverters.toBytes(cluster.lindex(JedisConverters.toString(key), index));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		try {
			return cluster.linsert(JedisConverters.toString(key), JedisConverters.toListPosition(where),
					JedisConverters.toString(pivot), JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		try {
			cluster.lset(JedisConverters.toString(key), index, JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		try {
			return cluster.lrem(JedisConverters.toString(key), count, JedisConverters.toString(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lPop(byte[] key) {

		try {
			return JedisConverters.toBytes(cluster.lpop(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] rPop(byte[] key) {

		try {
			return JedisConverters.toBytes(cluster.rpop(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException("this is only possible of both keys map to the same slot");
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException("this is only possible of both keys map to the same slot");
	}

	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		try {
			return cluster.sadd(JedisConverters.toString(key), JedisConverters.toStrings(values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sRem(byte[] key, byte[]... values) {

		try {
			return cluster.srem(JedisConverters.toString(key), JedisConverters.toStrings(values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] sPop(byte[] key) {
		try {
			return JedisConverters.toBytes(cluster.spop(JedisConverters.toString(key)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		throw new UnsupportedOperationException("Only possible if both map to the same slot");
	}

	@Override
	public Long sCard(byte[] key) {

		try {
			return cluster.scard(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] sRandMember(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zRem(byte[] key, byte[]... values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long begin, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long begin, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long begin, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zCard(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zRemRange(byte[] key, long begin, long end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		// TODO Auto-generated method stub

	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long hLen(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<byte[]> hVals(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void multi() {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Object> exec() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void discard() {
		// TODO Auto-generated method stub

	}

	@Override
	public void watch(byte[]... keys) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unwatch() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isSubscribed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Subscription getSubscription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long publish(byte[] channel, byte[] message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		// TODO Auto-generated method stub

	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		// TODO Auto-generated method stub

	}

	@Override
	public void select(final int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] echo(final byte[] message) {

		return new JedisClusterCommand<byte[]>(getClusterConnectionHandler(), 1, 5) {

			@Override
			public byte[] execute(Jedis connection) {
				return connection.echo(message);
			}
		}.run(null);
	}

	@Override
	public String ping() {

		return !runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.ping();
			}
		}).isEmpty() ? "PONG" : null;

	}

	@Override
	public String ping(RedisNode node) {

		return runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.ping();
			}
		}, node);
	}

	@Override
	public void bgWriteAof() {

		runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.bgrewriteaof();
			}
		});
	}

	@Override
	public void bgRewriteAof(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.bgrewriteaof();
			}
		}, node);
	}

	@Override
	public void bgReWriteAof() {

		runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.bgrewriteaof();
			}
		});
	}

	@Override
	public void bgSave() {

		runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.bgsave();
			}
		});
	}

	@Override
	public void bgSave(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInJedis(Jedis jedis) {
				jedis.bgsave();
				return null;
			}
		}, node);
	}

	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<Long>(runCommandOnAllNodes(new JedisCommandCallback<Long>() {

			@Override
			public Long doInJedis(Jedis jedis) {
				return jedis.lastsave();
			}
		}).values());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	@Override
	public Long lastSave(RedisNode node) {

		return runCommandOnSingleNode(new JedisCommandCallback<Long>() {

			@Override
			public Long doInJedis(Jedis jedis) {
				return jedis.lastsave();
			}
		}, node);
	}

	@Override
	public void save() {

		runCommandOnAllNodes(new JedisCommandCallback<Void>() {

			@Override
			public Void doInJedis(Jedis jedis) {
				jedis.save();
				return null;
			}
		});
	}

	@Override
	public void save(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.save();
			}
		}, node);

	}

	@Override
	public Long dbSize() {

		Map<RedisNode, Long> dbSizes = runCommandOnAllNodes(new JedisCommandCallback<Long>() {

			@Override
			public Long doInJedis(Jedis jedis) {
				return jedis.dbSize();
			}
		});

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes.values()) {
			size += value;
		}
		return size;
	}

	@Override
	public Long dbSize(RedisNode node) {

		return runCommandOnSingleNode(new JedisCommandCallback<Long>() {

			@Override
			public Long doInJedis(Jedis jedis) {
				return jedis.dbSize();
			}
		}, node);
	}

	@Override
	public void flushDb() {

		runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.flushDB();
			}
		});
	}

	@Override
	public void flushDb(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.flushDB();
			}
		}, node);
	}

	@Override
	public void flushAll() {

		runCommandOnAllNodes(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.flushAll();
			}
		});
	}

	@Override
	public void flushAll(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<String>() {

			@Override
			public String doInJedis(Jedis jedis) {
				return jedis.flushAll();
			}
		}, node);
	}

	@Override
	public Properties info() {

		Properties infos = new Properties();

		infos.putAll(runCommandOnAllNodes(new JedisCommandCallback<Properties>() {

			@Override
			public Properties doInJedis(Jedis jedis) {
				return JedisConverters.toProperties(jedis.info());
			}
		}));

		return infos;
	}

	@Override
	public Properties info(RedisNode node) {

		return runCommandOnSingleNode(new JedisCommandCallback<Properties>() {

			@Override
			public Properties doInJedis(Jedis jedis) {
				return JedisConverters.toProperties(jedis.info());
			}
		}, node);
	}

	@Override
	public Properties info(String section) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void shutdown() {

		runCommandOnAllNodes(new JedisCommandCallback<Void>() {

			@Override
			public Void doInJedis(Jedis jedis) {
				jedis.shutdown();
				return null;
			}
		});
	}

	@Override
	public void shutdown(RedisNode node) {

		runCommandOnSingleNode(new JedisCommandCallback<Void>() {

			@Override
			public Void doInJedis(Jedis jedis) {
				jedis.shutdown();
				return null;
			}
		}, node);

	}

	@Override
	public void shutdown(ShutdownOption option) {
		throw new UnsupportedOperationException("TODO: can be done using eval and shutdown script");
	}

	@Override
	public List<String> getConfig(String pattern) {
		throw new UnsupportedOperationException("Cannot get config from multiple Nodes since return type does not match");
	}

	@Override
	public void setConfig(String param, String value) {
		throw new UnsupportedOperationException("Cannot get config from multiple Nodes since return type does not match");
	}

	@Override
	public void resetConfigStats() {
		// TODO Auto-generated method stub

	}

	@Override
	public Long time() {
		throw new UnsupportedOperationException("Need to use a single host to do so");
	}

	@Override
	public void killClient(String host, int port) {
		throw new UnsupportedOperationException("Requires to have a specific client.");

	}

	@Override
	public void setClientName(byte[] name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getClientName() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<RedisClientInfo> getClientList() {

		Map<RedisNode, List<RedisClientInfo>> map = runCommandOnAllNodes(new JedisCommandCallback<List<RedisClientInfo>>() {

			@Override
			public List<RedisClientInfo> doInJedis(Jedis jedis) {
				return JedisConverters.toListOfRedisClientInformation(jedis.clientList());
			}
		});

		ArrayList<RedisClientInfo> result = new ArrayList<RedisClientInfo>();
		for (List<RedisClientInfo> infos : map.values()) {
			result.addAll(infos);
		}
		return result;

	}

	@Override
	public void slaveOf(String host, int port) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void slaveOfNoOne() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scriptFlush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void scriptKill() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String scriptLoad(byte[] script) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Boolean> scriptExists(String... scriptShas) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		try {
			return cluster.pfadd(JedisConverters.toString(key), JedisConverters.toStrings(values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pfCount(byte[]... keys) {
		throw new UnsupportedOperationException("well that would work if keys are on same slot");
	}

	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
		throw new UnsupportedOperationException("well that would work if keys are on same slot");
	}

	@Override
	public Boolean exists(final byte[] key) {
		return runClusterCommand(new JedisClusterCommand<Boolean>(getClusterConnectionHandler(), 1, 5) {

			@Override
			public Boolean execute(Jedis connection) {
				return connection.exists(key);
			}
		}, key);
	}

	private <T> T runClusterCommand(JedisClusterCommand<T> cmd, byte[] key) {
		try {
			return cmd.run(JedisConverters.toString(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private <T> T runCommandOnSingleNode(JedisCommandCallback<T> cmd, RedisNode node) {

		JedisPool pool = getResourcePoolForSpecificNode(node);
		notNull(pool, "Node not know in cluster. Is your cluster info up to date");

		boolean broken = false;

		Jedis jedis = pool.getResource();
		try {
			return cmd.doInJedis(jedis);
		} catch (Exception ex) {
			broken = ex instanceof NullPointerException;
			RuntimeException translatedException = convertJedisAccessException(ex);
			if (!broken && translatedException instanceof RedisConnectionFailureException) {
				broken = true;
			}
			throw translatedException;
		} finally {
			if (broken) {
				pool.returnBrokenResource(jedis);
			} else {
				pool.returnResource(jedis);
			}
		}
	}

	private <T> Map<RedisNode, T> runCommandOnAllNodes(final JedisCommandCallback<T> cmd) {

		List<RedisNode> nodes = getClusterNodes();
		// TODO: check for which number of nodes it makes sense to run this async
		return runCommandAsyncOnAllNodes(cmd, nodes);
	}

	private <T> Map<RedisNode, T> runCommandAsyncOnAllNodes(final JedisCommandCallback<T> cmd, Iterable<RedisNode> nodes) {

		final Map<RedisNode, T> result = new LinkedHashMap<RedisNode, T>();
		List<Future<T>> futures = new ArrayList<Future<T>>();
		for (final RedisNode node : nodes) {

			futures.add(executor.submit(new Callable<T>() {

				@Override
				public T call() throws Exception {
					return result.put(node, runCommandOnSingleNode(cmd, node));
				}

			}));
		}

		boolean done = false;
		while (!done) {

			done = true;
			for (Future<T> future : futures) {
				if (!future.isDone()) {
					done = false;
				}
			}
		}
		return result;
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	@Override
	public List<RedisNode> getClusterNodes() {

		List<RedisNode> nodes = new ArrayList<RedisNode>();
		Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
		for (String hostAndPort : clusterNodes.keySet()) {

			String[] args = StringUtils.split(hostAndPort, ":");
			nodes.add(new RedisNode(args[0], Integer.valueOf(args[1])));
		}

		return nodes;
	}

	protected JedisPool getResourcePoolForSpecificNode(RedisNode node) {

		notNull(node);
		Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
		if (clusterNodes.containsKey(node.asString())) {
			return clusterNodes.get(node.asString());
		}
		return null;
	}

	private JedisClusterConnectionHandler getClusterConnectionHandler() {
		return (JedisClusterConnectionHandler) getField(CONNECTION_HANDLER, cluster);
	}

	interface JedisCommandCallback<T> {
		T doInJedis(Jedis jedis);
	}

}
