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
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisCommandInterruptedException;
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
 * @author Jennifer Hickey
 */
public class LettuceConnection implements RedisConnection {

	private final com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> asyncSharedConn;
	private final com.lambdaworks.redis.RedisConnection<byte[], byte[]> sharedConn;
	private com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> asyncDedicatedConn;
	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> dedicatedConn;

	private final RedisCodec<byte[], byte[]> codec = LettuceUtils.CODEC;
	private final long timeout;

	// refers only to main connection as pubsub happens on a different one
	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean isPipelined = false;
	private List<Command<?, ?, ?>> ppline;
	private RedisClient client;
	private volatile LettuceSubscription subscription;
	private Pool<RedisAsyncConnection<byte[], byte[]>> pool;
	/** flag indicating whether the connection needs to be dropped or not */
	private boolean broken = false;

	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> dedicatedConnection, long timeout,
			RedisClient client) {
		this(null, dedicatedConnection, timeout, client, null);
	}

	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> dedicatedConnection, long timeout,
			RedisClient client, Pool<RedisAsyncConnection<byte[], byte[]>> pool) {
		this(null, dedicatedConnection, timeout, client, pool);
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param connection underlying Lettuce async connection
	 * @param timeout the connection timeout (in milliseconds)
	 * @param client The Lettuce client
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection,
			com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> dedicatedConnection, long timeout,
			RedisClient client) {
		this(sharedConnection, dedicatedConnection, timeout, client, null);
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param connection The underlying Lettuce async connection
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The Lettuce client
	 * @param closeNativeConnection True if native connection should be called on {@link #close()}. This should
	 * be set to false if the native connection is shared.
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection,
			com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> dedicatedConnection, long timeout,
			RedisClient client, Pool<RedisAsyncConnection<byte[], byte[]>> pool) {
		Assert.notNull(dedicatedConnection, "a valid dedicated connection is required");
		this.asyncSharedConn = sharedConnection;
		this.asyncDedicatedConn = dedicatedConnection;
		this.timeout = timeout;
		this.sharedConn = sharedConnection != null ?
				new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(asyncSharedConn) : null;
		this.dedicatedConn = new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(dedicatedConnection);
		this.client = client;
		this.pool = pool;
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			if(!(ex instanceof RedisCommandInterruptedException)) {
				// Some connection issues (i.e. connection is closed) are thrown as RedisExceptions
				broken = true;
			}
			return LettuceUtils.convertRedisAccessException((RedisException) ex);
		}
		if (ex instanceof ChannelException) {
			broken = true;
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
		return getAsyncConnection().await(cmd, timeout, TimeUnit.MILLISECONDS);
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
				pipeline(getAsyncConnection().dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(codec), cmdArg));
				return null;
			}
			else {
				return await(getAsyncConnection().dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(codec), cmdArg));
			}
		} catch (RedisException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void close() throws DataAccessException {
		isClosed = true;

		if(pool != null) {
			if (!broken) {
				pool.returnResource(asyncDedicatedConn);
			}else {
				pool.returnBrokenResource(asyncDedicatedConn);
			}
			return;
		}

		try {
			asyncDedicatedConn.close();
		} catch (RuntimeException ex) {
			throw convertLettuceAccessException(ex);
		}

		if (subscription != null) {
			if(subscription.isAlive()) {
				subscription.doClose();
			}
			subscription = null;
		}
	}

	public boolean isClosed() {
		return isClosed && !isSubscribed();
	}

	public RedisAsyncConnection<byte[], byte[]> getNativeConnection() {
		return (subscription != null ? subscription.pubsub : getAsyncConnection());
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
			boolean done = getAsyncConnection().awaitAll(ppline.toArray(new Command[ppline.size()]));
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
				pipeline(getAsyncConnection().sort(key, args));
				return null;
			}
			return getConnection().sort(key, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		SortArgs args = LettuceUtils.sort(params);

		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sortStore(key, args, sortKey));
				return null;
			}
			return getConnection().sortStore(key, args, sortKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().dbsize());
				return null;
			}
			return getConnection().dbsize();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().flushdb());
				return;
			}
			getConnection().flushdb();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().flushall());
				return;
			}
			getConnection().flushall();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().bgsave());
				return;
			}
			getConnection().bgsave();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void bgWriteAof() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().bgrewriteaof());
				return;
			}
			getConnection().bgrewriteaof();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void save() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().save());
				return;
			}
			getConnection().save();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().configGet(param));
				return null;
			}
			return getConnection().configGet(param);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().info());
				return null;
			}
			return LettuceUtils.info(getConnection().info());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lastsave());
				return null;
			}
			return getConnection().lastsave().getTime();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().configSet(param, value));
				return;
			}
			getConnection().configSet(param, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().configResetstat());
				return;
			}
			getConnection().configResetstat();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void shutdown() {
		try {
			if (isPipelined()) {
				getAsyncConnection().shutdown(true);
				return;
			}
			getConnection().shutdown(true);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().echo(message));
				return null;
			}
			return getConnection().echo(message);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().ping());
				return null;
			}
			return getConnection().ping();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long del(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().del(keys));
				return null;
			}
			return getConnection().del(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void discard() {
		isMulti = false;
		try {
			if (isPipelined()) {
				pipeline(getAsyncTxConnection().discard());
				return;
			}
			getTxConnection().discard();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<Object> exec() {
		isMulti = false;
		try {
			if (isPipelined()) {
				 getAsyncTxConnection().exec();
				 return null;
			}
			List<Object> results = getTxConnection().exec();
			if(results.isEmpty()) {
				return null;
			}
			return results;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().exists(key));
				return null;
			}
			return getConnection().exists(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().expire(key, seconds));
				return null;
			}
			return getConnection().expire(key, seconds);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().expireat(key, unixTime));
				return null;
			}
			return getConnection().expireat(key, unixTime);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean pExpire(byte[] key, long millis) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().pexpire(key, millis));
				return null;
			}
			return getConnection().pexpire(key, millis);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().pexpireat(key, unixTimeInMillis));
				return null;
			}
			return getConnection().pexpireat(key, unixTimeInMillis);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long pTtl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().pttl(key));
				return null;
			}
			return getConnection().pttl(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] dump(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().dump(key));
				return null;
			}
			return getConnection().dump(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().restore(key, ttlInMillis, serializedValue));
				return;
			}
			getConnection().restore(key, ttlInMillis, serializedValue);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> keys(byte[] pattern) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().keys(pattern));
				return null;
			}
			final List<byte[]> results = getConnection().keys(pattern);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		try {
			if (isPipelined()) {
				pipeline(getAsyncTxConnection().multi());
				return;
			}
			getTxConnection().multi();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().persist(key));
				return null;
			}
			return getConnection().persist(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean move(byte[] key, int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().move(key, dbIndex));
				return null;
			}
			return getConnection().move(key, dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().randomkey());
				return null;
			}
			return getConnection().randomkey();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void rename(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().rename(oldName, newName));
				return;
			}
			getConnection().rename(oldName, newName);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().renamenx(oldName, newName));
				return null;
			}
			return (getConnection().renamenx(oldName, newName));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void select(int dbIndex) {
		if(asyncSharedConn != null) {
			throw new UnsupportedOperationException("Selecting a new database not supported due to shared connection. " +
				"Use separate ConnectionFactorys to work with multiple databases");
		}
		try {
			if (isPipelined()) {
				throw new UnsupportedOperationException("Lettuce blocks for #select");
			}
			getConnection().select(dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().ttl(key));
				return null;
			}
			return getConnection().ttl(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public DataType type(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().type(key));
				return null;
			}
			final String type = getConnection().type(key);
			return type != null ? DataType.fromCode(type) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncTxConnection().unwatch());
				return;
			}
			getTxConnection().unwatch();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void watch(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncTxConnection().watch(keys));
				return;
			}
			else {
				getTxConnection().watch(keys);
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
				pipeline(getAsyncConnection().get(key));
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
				pipeline(getAsyncConnection().set(key, value));
				return;
			}
			getConnection().set(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}



	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().getset(key, value));
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
				pipeline(getAsyncConnection().append(key, value));
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
				pipeline(getAsyncConnection().mget(keys));
				return null;
			}
			return getConnection().mget(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().mset(tuples));
				return;
			}
			getConnection().mset(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().msetnx(tuples));
				return;
			}
			getConnection().msetnx(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().setex(key, time, value));
				return;
			}
			getConnection().setex(key, time, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().setnx(key, value));
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
				pipeline(getAsyncConnection().getrange(key, start, end));
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
				pipeline(getAsyncConnection().decr(key));
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
				pipeline(getAsyncConnection().decrby(key, value));
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
				pipeline(getAsyncConnection().incr(key));
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
				pipeline(getAsyncConnection().incrby(key, value));
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
				pipeline(getAsyncConnection().incrbyfloat(key, value));
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
				pipeline(getAsyncConnection().getbit(key, offset));
				return null;
			}
			final Long result = getConnection().getbit(key, offset);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().setbit(key, offset, LettuceUtils.asBit(value)));
			}
			getConnection().setbit(key, offset, LettuceUtils.asBit(value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().setrange(key, start, value));
			}
			getConnection().setrange(key, start, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().strlen(key));
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
				pipeline(getAsyncConnection().bitcount(key));
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
				pipeline(getAsyncConnection().bitcount(key, begin, end));
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
				pipeline(asyncBitOp(op, destination, keys));
				return null;
			}
			return syncBitOp(op, destination, keys);
		} catch (UnsupportedOperationException ex) {
			throw ex;
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
				pipeline(getAsyncConnection().lpush(key, value));
				return null;
			}
			return getConnection().lpush(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long rPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().rpush(key, value));
				return null;
			}
			return getConnection().rpush(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncBlockingConnection().blpop(timeout, keys));
				return null;
			}
			return LettuceUtils.toList(getBlockingConnection().blpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncBlockingConnection().brpop(timeout, keys));
				return null;
			}
			return LettuceUtils.toList(getBlockingConnection().brpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lindex(key, index));
				return null;
			}
			return getConnection().lindex(key, index);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().linsert(key, LettuceUtils.convertPosition(where), pivot, value));
				return null;
			}
			return getConnection().linsert(key, LettuceUtils.convertPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().llen(key));
				return null;
			}
			return getConnection().llen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] lPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lpop(key));
				return null;
			}
			return getConnection().lpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lrange(key, start, end));
				return null;
			}
			return getConnection().lrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lrem(key, count, value));
				return null;
			}
			return getConnection().lrem(key, count, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lset(key, index, value));
				return;
			}
			getConnection().lset(key, index, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().ltrim(key, start, end));
				return;
			}
			getConnection().ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] rPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().rpop(key));
				return null;
			}
			return getConnection().rpop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().rpoplpush(srcKey, dstKey));
				return null;
			}
			return getConnection().rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncBlockingConnection().brpoplpush(timeout, srcKey, dstKey));
				return null;
			}
			return getBlockingConnection().brpoplpush(timeout, srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().lpushx(key, value));
				return null;
			}
			return getConnection().lpushx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().rpushx(key, value));
				return null;
			}
			return getConnection().rpushx(key, value);
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
				pipeline(getAsyncConnection().sadd(key, value));
				return null;
			}
			final Long result = getConnection().sadd(key, value);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().scard(key));
				return null;
			}
			return getConnection().scard(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sdiff(keys));
				return null;
			}
			return getConnection().sdiff(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sdiffstore(destKey, keys));
				return null;
			}
			return getConnection().sdiffstore(destKey, keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sinter(keys));
				return null;
			}
			return getConnection().sinter(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sinterstore(destKey, keys));
				return null;
			}
			return getConnection().sinterstore(destKey, keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sismember(key, value));
				return null;
			}
			return getConnection().sismember(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().smembers(key));
				return null;
			}
			return getConnection().smembers(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().smove(srcKey, destKey, value));
				return null;
			}
			return getConnection().smove(srcKey, destKey, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().spop(key));
				return null;
			}
			return getConnection().spop(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().srandmember(key));
				return null;
			}
			return getConnection().srandmember(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean sRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().srem(key, value));
				return null;
			}
			final Long result = getConnection().srem(key, value);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sunion(keys));
				return null;
			}
			return getConnection().sunion(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().sunionstore(destKey, keys));
				return null;
			}
			return getConnection().sunionstore(destKey, keys);
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
				pipeline(getAsyncConnection().zadd(key, score, value));
				return null;
			}
			final Long result = getConnection().zadd(key, score, value);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zcard(key));
				return null;
			}
			return getConnection().zcard(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zcount(key, min, max));
				return null;
			}
			return getConnection().zcount(key, min, max);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zincrby(key, increment, value));
				return null;
			}
			return getConnection().zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = LettuceUtils.zArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zinterstore(destKey, storeArgs, sets));
				return null;
			}
			return getConnection().zinterstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zInterStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zinterstore(destKey, sets));
				return null;
			}
			return getConnection().zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrange(key, start, end));
				return null;
			}
			final List<byte[]> results = getConnection().zrange(key, start, end);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrangeWithScores(key, start, end));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrangebyscore(key, min, max));
				return null;
			}
			final List<byte[]> results = getConnection().zrangebyscore(key, min, max);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrangebyscoreWithScores(key, min, max));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrangebyscoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrangeWithScores(key, start, end));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrangebyscore(key, min, max, offset, count));
				return null;
			}
			final List<byte[]> results = getConnection().zrangebyscore(key, min, max, offset, count);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrangebyscoreWithScores(key, min, max, offset, count));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrangebyscoreWithScores(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrangebyscore(key, max, min, offset, count));
				return null;
			}
			final List<byte[]> results = getConnection().zrevrangebyscore(key, max, min, offset, count);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrangebyscore(key, max, min));
				return null;
			}
			final List<byte[]> results = getConnection().zrevrangebyscore(key, max, min);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min, offset, count));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrevrangebyscoreWithScores(key, max, min, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min));
				return null;
			}
			return LettuceUtils.convertTuple(getConnection().zrevrangebyscoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrank(key, value));
				return null;
			}
			return getConnection().zrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean zRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrem(key, value));
				return null;
			}
			final Long result = getConnection().zrem(key, value);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zremrangebyrank(key, start, end));
				return null;
			}
			return getConnection().zremrangebyrank(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zremrangebyscore(key, min, max));
				return null;
			}
			return getConnection().zremrangebyscore(key, min, max);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrange(key, start, end));
				return null;
			}
			final List<byte[]> results = getConnection().zrevrange(key, start, end);
			return results != null ? new LinkedHashSet<byte[]>(results) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zrevrank(key, value));
				return null;
			}
			return getConnection().zrevrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double zScore(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zscore(key, value));
				return null;
			}
			return getConnection().zscore(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = LettuceUtils.zArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zunionstore(destKey, storeArgs, sets));
				return null;
			}
			return getConnection().zunionstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().zunionstore(destKey, sets));
				return null;
			}
			return getConnection().zunionstore(destKey, sets);
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
				pipeline(getAsyncConnection().hset(key, field, value));
				return null;
			}
			return getConnection().hset(key, field, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hsetnx(key, field, value));
				return null;
			}
			return getConnection().hsetnx(key, field, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hDel(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hdel(key, field));
				return null;
			}
			final Long result = getConnection().hdel(key, field);
			return result != null ? result == 1 : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hexists(key, field));
				return null;
			}
			return getConnection().hexists(key, field);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public byte[] hGet(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hget(key, field));
				return null;
			}
			return getConnection().hget(key, field);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hgetall(key));
				return null;
			}
			return getConnection().hgetall(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hincrby(key, field, delta));
				return null;
			}
			return getConnection().hincrby(key, field, delta);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hincrbyfloat(key, field, delta));
				return null;
			}
			return getConnection().hincrbyfloat(key, field, delta);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hkeys(key));
				return null;
			}
			final List<byte[]> result = getConnection().hkeys(key);
			return result != null ? new LinkedHashSet<byte[]>(result) : null;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hlen(key));
				return null;
			}
			return getConnection().hlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hmget(key, fields));
				return null;
			}
			return getConnection().hmget(key, fields);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hmset(key, tuple));
				return;
			}
			getConnection().hmset(key, tuple);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public List<byte[]> hVals(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().hvals(key));
				return null;
			}
			return getConnection().hvals(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	 //
	 // Scripting commands
	 //

	public void scriptFlush() {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().scriptFlush());
				return;
			}
			getConnection().scriptFlush();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void scriptKill() {
		if(isQueueing()) {
			throw new UnsupportedOperationException("Script kill not permitted in a transaction");
		}
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().scriptKill());
				return;
			}
			getConnection().scriptKill();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public String scriptLoad(byte[] script) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().scriptLoad(script));
				return null;
			}
			return getConnection().scriptLoad(script);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<Boolean> scriptExists(String... scriptSha1) {
		try {
			if (isPipelined()) {
				pipeline(getAsyncConnection().scriptExists(scriptSha1));
				return null;
			}
			return getConnection().scriptExists(scriptSha1);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			byte[][] keys = LettuceUtils.extractScriptKeys(numKeys, keysAndArgs);
			byte[][] args = LettuceUtils.extractScriptArgs(numKeys, keysAndArgs);

			if (isPipelined()) {
				pipeline(getAsyncConnection().eval(script, LettuceUtils.toScriptOutputType(returnType), keys, args));
				return null;
			}
			return getConnection().eval(script, LettuceUtils.toScriptOutputType(returnType), keys, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			byte[][] keys = LettuceUtils.extractScriptKeys(numKeys, keysAndArgs);
			byte[][] args = LettuceUtils.extractScriptArgs(numKeys, keysAndArgs);

			if (isPipelined()) {
				pipeline(getAsyncConnection().evalsha(scriptSha1, LettuceUtils.toScriptOutputType(returnType),
						keys, args));
				return null;
			}
			return getConnection().evalsha(scriptSha1, LettuceUtils.toScriptOutputType(returnType), keys, args);
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
				pipeline(getAsyncConnection().publish(channel, message));
				return null;
			}
			return getConnection().publish(channel, message);
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

	@SuppressWarnings("rawtypes")
	private void pipeline(Future<?> command) {
		// the future will always be a command plus it throws no exception on #get
		ppline.add((Command) command);
	}

	private RedisAsyncConnection<byte[], byte[]> getAsyncConnection() {
		if(isQueueing()) {
			return getAsyncTxConnection();
		}
		if(asyncSharedConn != null) {
			return asyncSharedConn;
		}
		return asyncDedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getConnection() {
		if(isQueueing()) {
			return getTxConnection();
		}
		if(sharedConn != null) {
			return sharedConn;
		}
		return dedicatedConn;
	}

	private RedisAsyncConnection<byte[], byte[]> getAsyncBlockingConnection() {
		return asyncDedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getBlockingConnection() {
		return dedicatedConn;
	}

	private RedisAsyncConnection<byte[], byte[]> getAsyncTxConnection() {
		return asyncDedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getTxConnection() {
		return dedicatedConn;

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
}