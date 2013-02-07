/*
 * Copyright 2011-2013 the original author or authors.
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

import static com.lambdaworks.redis.protocol.CommandType.MULTI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jboss.netty.channel.ChannelException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.SortArgs;
import com.lambdaworks.redis.ZStoreArgs;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.output.ByteArrayOutput;
import com.lambdaworks.redis.protocol.Command;
import com.lambdaworks.redis.protocol.CommandArgs;
import com.lambdaworks.redis.protocol.CommandType;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/wg/lettuce">Lettuce</a> Redis client.
 * 
 * @author Costin Leau
 */
public class LettuceConnection implements RedisConnection {

	private final com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> asyncConn;
	private final com.lambdaworks.redis.RedisConnection<byte[], byte[]> con;
	private RedisPubSubConnection<byte[], byte[]> pubsub;
	private final RedisCodec<byte[], byte[]> codec = LettuceUtils.CODEC;
	private final long timeout;

	// refers only to main connection as pubsub happens on a different one
	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean isPipelined = false;
	private List<Command<?, ?, ?>> ppline;
	private RedisClient client;
	private volatile LettuceSubscription subscription;

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param connection underlying Lettuce async connection
	 * @param timeout the connection timeout (in milliseconds)
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> connection, long timeout,
			RedisClient client) {
		Assert.notNull(connection, "a valid connection is required");

		this.asyncConn = connection;
		this.timeout = timeout;
		this.con = new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(asyncConn);
		this.client = client;
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return LettuceUtils.convertRedisAccessException((RedisException) ex);
		}
		if (ex instanceof ChannelException) {
			return new RedisConnectionFailureException("Redis connection failed", ex);
		}
		if (ex instanceof TimeoutException) {
			return new QueryTimeoutException("Redis command timed out", ex);
		}
		return new RedisSystemException("Unknown Lettuce exception", ex);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object await(Command cmd) {
		if (isMulti && cmd.type != MULTI) {
			return null;
		}
		return asyncConn.await(cmd, timeout, TimeUnit.MILLISECONDS);
	}

	public Object execute(String command, byte[]... args) {
		Assert.hasText(command, "a valid command needs to be specified");
		try {
			String name = command.trim().toUpperCase();
			CommandType cmd = CommandType.valueOf(name);
			CommandArgs<byte[], byte[]> cmdArg = new CommandArgs<byte[], byte[]>(codec);

			if (!ObjectUtils.isEmpty(args)) {
				cmdArg.addKeys(args);
			}

			if (isPipelined()) {
				pipeline(asyncConn.dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(codec), cmdArg));
				return null;
			}
			else {
				return await(asyncConn.dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(codec), cmdArg));
			}
		} catch (RedisException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void close() throws DataAccessException {
		isClosed = true;

		try {
			asyncConn.close();
		} catch (RuntimeException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public boolean isClosed() {
		return isClosed && !isSubscribed();
	}

	public RedisAsyncConnection<byte[], byte[]> getNativeConnection() {
		return (pubsub != null ? pubsub : asyncConn);
	}


	public boolean isQueueing() {
		return isMulti;
	}

	public boolean isPipelined() {
		return isPipelined;
	}


	public void openPipeline() {
		if (!isPipelined) {
			isPipelined = true;
			ppline = new ArrayList<Command<?, ?, ?>>();
		}
	}

	public List<Object> closePipeline() {
		if (isPipelined) {
			isPipelined = false;
			boolean done = asyncConn.awaitAll(ppline.toArray(new Command[ppline.size()]));
			List<Object> results = new ArrayList<Object>(ppline.size());

			Exception problem = null;
			
			if (done) {
				for (Command<?, ?, ?> cmd : ppline) {
					if (cmd.getOutput().hasError()) {
						Exception err = new InvalidDataAccessApiUsageException(cmd.getOutput().getError());
						// remember only the first error
						if (problem == null) {
							problem = err;
						}
						results.add(err);
					}
					else {
						results.add(cmd.get());
					}
				}
			}
			ppline.clear();

			if (problem != null) {
				throw new RedisPipelineException(problem, results);
			}
			if (done) {
				return results;
			}

			throw new RedisPipelineException(new QueryTimeoutException("Redis command timed out"));
		}

		return Collections.emptyList();
	}


	public List<byte[]> sort(byte[] key, SortParameters params) {

		SortArgs args = LettuceUtils.sort(params);

		try {
			if (isPipelined()) {
				pipeline(asyncConn.sort(key, args));
				return null;
			}
			return con.sort(key, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		SortArgs args = LettuceUtils.sort(params);

		try {
			if (isPipelined()) {
				pipeline(asyncConn.sortStore(key, args, sortKey));
				return null;
			}
			return con.sortStore(key, args, sortKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.dbsize());
				return null;
			}
			return con.dbsize();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.flushdb());
				return;
			}
			con.flushdb();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.flushall());
				return;
			}
			con.flushall();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.bgsave());
				return;
			}
			con.bgsave();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void bgWriteAof() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.bgrewriteaof());
				return;
			}
			con.bgrewriteaof();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void save() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.save());
				return;
			}
			con.save();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.configGet(param));
				return null;
			}
			return con.configGet(param);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.info());
				return null;
			}
			return LettuceUtils.info(con.info());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lastsave());
				return null;
			}
			return con.lastsave().getTime();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.configSet(param, value));
				return;
			}
			con.configSet(param, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.configResetstat());
				return;
			}
			con.configResetstat();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void shutdown() {
		try {
			if (isPipelined()) {
				asyncConn.shutdown(true);
				return;
			}
			con.shutdown(true);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.echo(message));
				return null;
			}
			return con.echo(message);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.ping());
			}
			return con.ping();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long del(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.del(keys));
				return null;
			}
			return con.del(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void discard() {
		isMulti = false;
		try {
			if (isPipelined()) {
				con.discard();
			}

			con.discard();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<Object> exec() {
		isMulti = false;
		try {
			return con.exec();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.exists(key));
				return null;
			}
			return con.exists(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.expire(key, seconds));
				return null;
			}
			return con.expire(key, seconds);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.expireat(key, unixTime));
				return null;
			}
			return con.expireat(key, unixTime);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> keys(byte[] pattern) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.keys(pattern));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.keys(pattern));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		openPipeline();
		try {
			if (isPipelined()) {
				con.multi();
				return;
			}
			con.multi();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.persist(key));
				return null;
			}
			return con.persist(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean move(byte[] key, int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.move(key, dbIndex));
				return null;
			}
			return con.move(key, dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.randomkey());
				return null;
			}
			return con.randomkey();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void rename(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.rename(oldName, newName));
				return;
			}
			con.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.renamenx(oldName, newName));
				return null;
			}
			return (con.renamenx(oldName, newName));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				asyncConn.select(dbIndex);
			}
			con.select(dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.ttl(key));
				return null;
			}
			return con.ttl(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public DataType type(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.type(key));
				return null;
			}
			return DataType.fromCode(con.type(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.unwatch());
			}

			con.unwatch();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void watch(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.watch(keys));
			}
			else {
				con.watch(keys);
			}
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// String commands
	//

	public byte[] get(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.get(key));
				return null;
			}

			return con.get(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void set(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.set(key, value));
				return;
			}
			con.set(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.getset(key, value));
				return null;
			}
			return con.getset(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long append(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.append(key, value));
				return null;
			}
			return con.append(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> mGet(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.mget(keys));
				;
				return null;
			}
			return con.mget(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.mset(tuples));
				return;
			}
			con.mset(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.msetnx(tuples));
				return;
			}
			con.msetnx(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.setex(key, time, value));
				return;
			}
			con.setex(key, time, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.setnx(key, value));
				return null;
			}
			return con.setnx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] getRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.getrange(key, start, end));
				return null;
			}
			return con.getrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long decr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.decr(key));
				return null;
			}
			return con.decr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long decrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.decrby(key, value));
				return null;
			}
			return con.decrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long incr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.incr(key));
				return null;
			}
			return con.incr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long incrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.incrby(key, value));
				return null;
			}
			return con.incrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isPipelined()) {
				asyncConn.getbit(key, offset);
				return null;
			}
			return Long.valueOf(1).equals(con.getbit(key, offset));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				asyncConn.setbit(key, offset, LettuceUtils.asBit(value));
			}
			con.setbit(key, offset, LettuceUtils.asBit(value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				asyncConn.setrange(key, start, value);
			}
			con.setrange(key, start, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.strlen(key));
				return null;
			}
			return con.strlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// List commands
	//


	public Long lPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lpush(key, value));
				return null;
			}
			return con.lpush(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long rPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.rpush(key, value));
				return null;
			}
			return con.rpush(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.blpop(timeout, keys));
				return null;
			}
			return LettuceUtils.toList(con.blpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.brpop(timeout, keys));
				return null;
			}
			return LettuceUtils.toList(con.brpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lindex(key, index));
				return null;
			}
			return con.lindex(key, index);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.linsert(key, LettuceUtils.convertPosition(where), pivot, value));
				return null;
			}
			return con.linsert(key, LettuceUtils.convertPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.llen(key));
				return null;
			}
			return con.llen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] lPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lpop(key));
				return null;
			}
			return con.lpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lrange(key, start, end));
				return null;
			}
			return con.lrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lrem(key, count, value));
				return null;
			}
			return con.lrem(key, count, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lset(key, index, value));
				return;
			}
			con.lset(key, index, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.ltrim(key, start, end));
				return;
			}
			con.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] rPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.rpop(key));
				return null;
			}
			return con.rpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.rpoplpush(srcKey, dstKey));
				return null;
			}
			return con.rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.brpoplpush(timeout, srcKey, dstKey));
				return null;
			}
			return con.brpoplpush(timeout, srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.lpushx(key, value));
				return null;
			}
			return con.lpushx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.rpushx(key, value));
				return null;
			}
			return con.rpushx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// Set commands
	//


	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sadd(key, value));
				return null;
			}
			return con.sadd(key, value) == 1;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.scard(key));
				return null;
			}
			return con.scard(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sdiff(keys));
				return null;
			}
			return con.sdiff(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sdiffstore(destKey, keys));
				return null;
			}
			return con.sdiffstore(destKey, keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sinter(keys));
				return null;
			}
			return con.sinter(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sinterstore(destKey, keys));
				return null;
			}
			return con.sinterstore(destKey, keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sismember(key, value));
				return null;
			}
			return con.sismember(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.smembers(key));
				return null;
			}
			return con.smembers(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.smove(srcKey, destKey, value));
				return null;
			}
			return con.smove(srcKey, destKey, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.spop(key));
				return null;
			}
			return con.spop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.srandmember(key));
				return null;
			}
			return con.srandmember(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean sRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.srem(key, value));
				return null;
			}
			return Long.valueOf(1).equals(con.srem(key, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sunion(keys));
				return null;
			}
			return con.sunion(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.sunionstore(destKey, keys));
				return null;
			}
			return con.sunionstore(destKey, keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// ZSet commands
	//


	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zadd(key, score, value));
				return null;
			}
			return Long.valueOf(1).equals(con.zadd(key, score, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zcard(key));
				return null;
			}
			return con.zcard(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isQueueing()) {
				pipeline(asyncConn.zcount(key, min, max));
				return null;
			}
			return con.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zincrby(key, increment, value));
				return null;
			}
			return con.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = LettuceUtils.zArgs(aggregate, weights);

		try {
			if (isQueueing()) {
				pipeline(asyncConn.zinterstore(destKey, storeArgs, sets));
				return null;
			}
			return con.zinterstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zInterStore(byte[] destKey, byte[]... sets) {
		try {
			if (isQueueing()) {
				pipeline(asyncConn.zinterstore(destKey, sets));
				return null;
			}
			return con.zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrange(key, start, end));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrangeWithScores(key, start, end));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrangebyscore(key, min, max));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrangebyscore(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrangebyscoreWithScores(key, min, max));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrangebyscoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrangeWithScores(key, start, end));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrangebyscore(key, min, max, offset, count));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrangebyscore(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrangebyscoreWithScores(key, min, max, offset, count));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrangebyscoreWithScores(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrangebyscore(key, min, max, offset, count));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrevrangebyscore(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrangebyscore(key, min, max));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrevrangebyscore(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrangebyscoreWithScores(key, min, max));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrevrangebyscoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrangebyscoreWithScores(key, min, max));
				return null;
			}
			return LettuceUtils.convertTuple(con.zrevrangebyscoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrank(key, value));
				return null;
			}
			return con.zrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean zRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrem(key, value));
				return null;
			}
			return Long.valueOf(1).equals(con.zrem(key, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zremrangebyrank(key, start, end));
				return null;
			}
			return con.zremrangebyrank(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zremrangebyscore(key, min, max));
				return null;
			}
			return con.zremrangebyscore(key, min, max);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrange(key, start, end));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.zrevrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zrevrank(key, value));
				return null;
			}
			return con.zrevrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double zScore(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zscore(key, value));
				return null;
			}
			return con.zscore(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = LettuceUtils.zArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(asyncConn.zunionstore(destKey, storeArgs, sets));
				return null;
			}
			return con.zunionstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.zunionstore(destKey, sets));
				return null;
			}
			return con.zunionstore(destKey, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// Hash commands
	//

	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hset(key, field, value));
				return null;
			}
			return con.hset(key, field, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hsetnx(key, field, value));
				return null;
			}
			return con.hsetnx(key, field, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hDel(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hdel(key, field));
				return null;
			}
			return Long.valueOf(1).equals(con.hdel(key, field));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hexists(key, field));
				return null;
			}
			return con.hexists(key, field);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] hGet(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hget(key, field));
				return null;
			}
			return con.hget(key, field);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hgetall(key));
				return null;
			}
			return con.hgetall(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hincrby(key, field, delta));
				return null;
			}
			return con.hincrby(key, field, delta);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hkeys(key));
				return null;
			}
			return new LinkedHashSet<byte[]>(con.hkeys(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hlen(key));
				return null;
			}
			return con.hlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hmget(key, fields));
				return null;
			}
			return con.hmget(key, fields);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hmset(key, tuple));
				return;
			}
			con.hmset(key, tuple);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<byte[]> hVals(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.hvals(key));
				return null;
			}
			return con.hvals(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	//
	// Pub/Sub functionality
	//

	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(asyncConn.publish(channel, message));
				return null;
			}
			return con.publish(channel, message);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Subscription getSubscription() {
		return subscription;
	}


	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}


	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		checkSubscription();

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new LettuceSubscription(listener, switchToPubSub());
			subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void subscribe(MessageListener listener, byte[]... channels) {
		checkSubscription();

		try {
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			subscription = new LettuceSubscription(listener, switchToPubSub());
			subscription.subscribe(channels);

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private void checkSubscription() {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
	}

	private RedisPubSubConnection<byte[], byte[]> switchToPubSub() {
		close();
		// open a pubsub one
		return client.connectPubSub(LettuceUtils.CODEC);
	}

	private void pipeline(Future<?> command) {
		// the future will always be a command plus it throws no exception on #get
		ppline.add((Command) command);
	}
}