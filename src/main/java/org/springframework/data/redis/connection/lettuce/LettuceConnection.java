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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
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
 * @author Jennifer Hickey
 */
public class LettuceConnection implements RedisConnection {

	static final RedisCodec<byte[], byte[]> CODEC = new BytesRedisCodec();

	private final com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> asyncSharedConn;
	private final com.lambdaworks.redis.RedisConnection<byte[], byte[]> sharedConn;
	private com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> asyncDedicatedConn;
	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> dedicatedConn;

	private final long timeout;

	// refers only to main connection as pubsub happens on a different one
	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean isPipelined = false;
	private List<LettuceResult> ppline;
	private Queue<FutureResult<?>> txResults = new LinkedList<FutureResult<?>>();
	private RedisClient client;
	private volatile LettuceSubscription subscription;
	private LettucePool pool;
	/** flag indicating whether the connection needs to be dropped or not */
	private boolean broken = false;
	private boolean convertPipelineAndTxResults=true;

	@SuppressWarnings("rawtypes")
	private class LettuceResult extends FutureResult<Command<?, ?, ?>> {
		public <T> LettuceResult(Future<T> resultHolder, Converter<T,?> converter) {
			super((Command)resultHolder, converter);
		}

		public LettuceResult(Future resultHolder) {
			super((Command)resultHolder);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			if(convertPipelineAndTxResults && converter != null) {
				return converter.convert(resultHolder.get());
			}
			return resultHolder.get();
		}
	}

	private class LettuceStatusResult extends LettuceResult {
		@SuppressWarnings("rawtypes")
		public LettuceStatusResult(Future resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	private class LettuceTxResult extends FutureResult<Object> {
		public LettuceTxResult(Object resultHolder, Converter<?,?> converter) {
			super(resultHolder, converter);
		}

		public LettuceTxResult(Object resultHolder) {
			super(resultHolder);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			if(convertPipelineAndTxResults && converter != null) {
				return converter.convert(resultHolder);
			}
			return resultHolder;
		}
	}

	private class LettuceTxStatusResult extends LettuceTxResult {
		public LettuceTxStatusResult(Object resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	private class LettuceTransactionResultConverter<T> extends TransactionResultConverter<T> {
		public LettuceTransactionResultConverter(Queue<FutureResult<T>> txResults,
				Converter<Exception, DataAccessException> exceptionConverter) {
			super(txResults, exceptionConverter);
		}

		@Override
		public List<Object> convert(List<Object> execResults) {
			// Lettuce Empty list means null (watched variable modified)
			if(execResults.isEmpty()) {
				return null;
			}
			return super.convert(execResults);
		}
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param timeout
	 *            The connection timeout (in milliseconds)
	 * @param client
	 *            The {@link RedisClient} to use when instantiating a native
	 *            connection
	 */
	public LettuceConnection(long timeout, RedisClient client) {
		this(null, timeout, client, null);
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param timeout
	 *            The connection timeout (in milliseconds) * @param client The
	 *            {@link RedisClient} to use when instantiating a pub/sub
	 *            connection
	 * @param pool
	 *            The connection pool to use for all other native connections
	 */
	public LettuceConnection(long timeout, RedisClient client, LettucePool pool) {
		this(null, timeout, client, pool);
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param sharedConnection
	 *            A native connection that is shared with other
	 *            {@link LettuceConnection}s. Will not be used for transactions
	 *            or blocking operations
	 * @param timeout
	 *            The connection timeout (in milliseconds)
	 * @param client
	 *            The {@link RedisClient} to use when making pub/sub, blocking,
	 *            and tx connections
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection,
			long timeout, RedisClient client) {
		this(sharedConnection, timeout, client, null);
	}

	/**
	 * Instantiates a new lettuce connection.
	 *
	 * @param sharedConnection
	 *            A native connection that is shared with other
	 *            {@link LettuceConnection}s. Should not be used for
	 *            transactions or blocking operations
	 * @param timeout
	 *            The connection timeout (in milliseconds)
	 * @param client
	 *            The {@link RedisClient} to use when making pub/sub connections
	 * @param pool
	 *            The connection pool to use for blocking and tx operations
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection,
			long timeout, RedisClient client, LettucePool pool) {
		this.asyncSharedConn = sharedConnection;
		this.timeout = timeout;
		this.sharedConn = sharedConnection != null ? new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(
				asyncSharedConn) : null;
		this.client = client;
		this.pool = pool;
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {
		DataAccessException exception = LettuceConverters.toDataAccessException(ex);
		if (exception instanceof RedisConnectionFailureException) {
			broken = true;
		}
		return exception;
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
			CommandArgs<byte[], byte[]> cmdArg = new CommandArgs<byte[], byte[]>(CODEC);

			if (!ObjectUtils.isEmpty(args)) {
				cmdArg.addKeys(args);
			}

			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(CODEC), cmdArg)));
				return null;
			} else if(isQueueing()) {
				transaction(new LettuceResult(getAsyncConnection().dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(CODEC), cmdArg)));
				return null;
			} else {
				return await(getAsyncConnection().dispatch(cmd, new ByteArrayOutput<byte[], byte[]>(CODEC), cmdArg));
			}
		} catch (RedisException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void close() throws DataAccessException {
		isClosed = true;

		if(asyncDedicatedConn != null) {
			if(pool != null) {
				if (!broken) {
					pool.returnResource(asyncDedicatedConn);
				}else {
					pool.returnBrokenResource(asyncDedicatedConn);
				}
			} else {
				try {
					asyncDedicatedConn.close();
				} catch (RuntimeException ex) {
					throw convertLettuceAccessException(ex);
				}
			}
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
			ppline = new ArrayList<LettuceResult>();
		}
	}

	public List<Object> closePipeline() {
		if (isPipelined) {
			isPipelined = false;
			List<Command<?, ?, ?>> futures = new ArrayList<Command<?, ?, ?>>();
			for(LettuceResult result: ppline) {
				futures.add(result.getResultHolder());
			}
			boolean done = getAsyncConnection().awaitAll(futures.toArray(new Command[futures.size()]));
			List<Object> results = new ArrayList<Object>(futures.size());

			Exception problem = null;

			if (done) {
				for (LettuceResult result: ppline) {
					if (result.getResultHolder().getOutput().hasError()) {
						Exception err = new InvalidDataAccessApiUsageException(result.getResultHolder().getOutput().getError());
						// remember only the first error
						if (problem == null) {
							problem = err;
						}
						results.add(err);
					}
					else if(!convertPipelineAndTxResults || !(result.isStatus())) {
						try {
							results.add(result.get());
						}catch(DataAccessException e) {
							if (problem == null) {
								problem = e;
							}
							results.add(e);
						}
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

		SortArgs args = LettuceConverters.toSortArgs(params);

		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().sort(key, args)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sort(key, args)));
				return null;
			}
			return getConnection().sort(key, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		SortArgs args = LettuceConverters.toSortArgs(params);

		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().sortStore(key, args, sortKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sortStore(key, args, sortKey)));
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
				pipeline(new LettuceResult(getAsyncConnection().dbsize()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().dbsize()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().flushdb()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().flushdb()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().flushall()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().flushall()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().bgsave()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().bgsave()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().bgrewriteaof()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().bgrewriteaof()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().save()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().save()));
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
				pipeline(new LettuceResult(getAsyncConnection().configGet(param)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().configGet(param)));
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
				pipeline(new LettuceResult(getAsyncConnection().info(), LettuceConverters.stringToProps()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().info(), LettuceConverters.stringToProps()));
				return null;
			}
			return LettuceConverters.toProperties(getConnection().info());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Properties info(String section) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().info(section), LettuceConverters.stringToProps()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().info(section), LettuceConverters.stringToProps()));
				return null;
			}
			return LettuceConverters.toProperties(getConnection().info(section));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().lastsave(), LettuceConverters.dateToLong()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lastsave(), LettuceConverters.dateToLong()));
				return null;
			}
			return LettuceConverters.toLong(getConnection().lastsave());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().configSet(param, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().configSet(param, value)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().configResetstat()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().configResetstat()));
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
				pipeline(new LettuceResult(getAsyncConnection().echo(message)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().echo(message)));
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
				pipeline(new LettuceResult(getAsyncConnection().ping()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().ping()));
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
				pipeline(new LettuceResult(getAsyncConnection().del(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().del(keys)));
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
				pipeline(new LettuceStatusResult(getAsyncDedicatedConnection().discard()));
				return;
			}
			getDedicatedConnection().discard();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		} finally {
			txResults.clear();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<Object> exec() {
		isMulti = false;
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncDedicatedConnection().exec(),
						new LettuceTransactionResultConverter(new LinkedList<FutureResult<?>>(txResults),
								LettuceConverters.exceptionConverter())));
				return null;
			}
			List<Object> results = getDedicatedConnection().exec();
			return convertPipelineAndTxResults ? new LettuceTransactionResultConverter(txResults,
					LettuceConverters.exceptionConverter()).convert(results) : results;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		} finally {
			txResults.clear();
		}
	}


	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().exists(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().exists(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().expire(key, seconds)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().expire(key, seconds)));
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
				pipeline(new LettuceResult(getAsyncConnection().expireat(key, unixTime)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().expireat(key, unixTime)));
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
				pipeline(new LettuceResult(getAsyncConnection().pexpire(key, millis)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().pexpire(key, millis)));
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
				pipeline(new LettuceResult(getAsyncConnection().pexpireat(key, unixTimeInMillis)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().pexpireat(key, unixTimeInMillis)));
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
				pipeline(new LettuceResult(getAsyncConnection().pttl(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().pttl(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().dump(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().dump(key)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().restore(key, ttlInMillis, serializedValue)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().restore(key, ttlInMillis, serializedValue)));
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
				pipeline(new LettuceResult(getAsyncConnection().keys(pattern), LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().keys(pattern), LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().keys(pattern));
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
				getAsyncDedicatedConnection().multi();
				return;
			}
			getDedicatedConnection().multi();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().persist(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().persist(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().move(key, dbIndex)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().move(key, dbIndex)));
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
				pipeline(new LettuceResult(getAsyncConnection().randomkey()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().randomkey()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().rename(oldName, newName)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().rename(oldName, newName)));
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
				pipeline(new LettuceResult(getAsyncConnection().renamenx(oldName, newName)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().renamenx(oldName, newName)));
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
		if (isPipelined()) {
			throw new UnsupportedOperationException("Lettuce blocks for #select");
		}
		try {
			if(isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().select(dbIndex)));
				return;
			}
			getConnection().select(dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().ttl(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().ttl(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().type(key), LettuceConverters.stringToDataType()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().type(key), LettuceConverters.stringToDataType()));
				return null;
			}
			return LettuceConverters.toDataType(getConnection().type(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncDedicatedConnection().unwatch()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getDedicatedConnection().unwatch()));
				return;
			}
			getDedicatedConnection().unwatch();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public void watch(byte[]... keys) {
		if(isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncDedicatedConnection().watch(keys)));
				return;
			}
			getDedicatedConnection().watch(keys);
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
				pipeline(new LettuceResult(getAsyncConnection().get(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().get(key)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().set(key, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().set(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().getset(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().getset(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().append(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().append(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().mget(keys)));
				return null;
			}
			if(isQueueing()) {
				transaction(new LettuceTxResult(getConnection().mget(keys)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().mset(tuples)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().mset(tuples)));
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
				pipeline(new LettuceResult(getAsyncConnection().msetnx(tuples)));
				return null;
			}
			if(isQueueing()) {
				transaction(new LettuceTxResult(getConnection().msetnx(tuples)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().setex(key, time, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().setex(key, time, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().setnx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().setnx(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().getrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().getrange(key, start, end)));
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
				pipeline(new LettuceResult(getAsyncConnection().decr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().decr(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().decrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().decrby(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().incr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().incr(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().incrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().incrby(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().incrbyfloat(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().incrbyfloat(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			return LettuceConverters.toBoolean(getConnection().getbit(key, offset));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().setbit(key, offset, LettuceConverters.toInt(value))));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().setbit(key, offset, LettuceConverters.toInt(value))));
				return;
			}
			getConnection().setbit(key, offset, LettuceConverters.toInt(value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().setrange(key, start, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().setrange(key, start, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().strlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().strlen(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().bitcount(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().bitcount(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().bitcount(key, begin, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().bitcount(key, begin, end)));
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
				pipeline(new LettuceResult(asyncBitOp(op, destination, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(syncBitOp(op, destination, keys)));
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


	public Long lPush(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().lpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lpush(key, values)));
				return null;
			}
			return getConnection().lpush(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long rPush(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().rpush(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().rpush(key, values)));
				return null;
			}
			return getConnection().rpush(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncDedicatedConnection().blpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getDedicatedConnection().blpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			return LettuceConverters.toBytesList(getDedicatedConnection().blpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncDedicatedConnection().brpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getDedicatedConnection().brpop(timeout, keys),
						LettuceConverters.keyValueToBytesList()));
				return null;
			}
			return LettuceConverters.toBytesList(getDedicatedConnection().brpop(timeout, keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().lindex(key, index)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lindex(key, index)));
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
				pipeline(new LettuceResult(getAsyncConnection().linsert(key,
						LettuceConverters.toBoolean(where), pivot, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().linsert(key,
						LettuceConverters.toBoolean(where), pivot, value)));
				return null;
			}
			return getConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().llen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().llen(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().lpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lpop(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().lrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lrange(key, start, end)));
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
				pipeline(new LettuceResult(getAsyncConnection().lrem(key, count, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lrem(key, count, value)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().lset(key, index, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().lset(key, index, value)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().ltrim(key, start, end)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().ltrim(key, start, end)));
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
				pipeline(new LettuceResult(getAsyncConnection().rpop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().rpop(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().rpoplpush(srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().rpoplpush(srcKey, dstKey)));
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
				pipeline(new LettuceResult(getAsyncDedicatedConnection().brpoplpush(timeout, srcKey, dstKey)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getDedicatedConnection().brpoplpush(timeout, srcKey, dstKey)));
				return null;
			}
			return getDedicatedConnection().brpoplpush(timeout, srcKey, dstKey);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().lpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().lpushx(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().rpushx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().rpushx(key, value)));
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


	public Long sAdd(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().sadd(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sadd(key, values)));
				return null;
			}
			return getConnection().sadd(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().scard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().scard(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().sdiff(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sdiff(keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().sdiffstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sdiffstore(destKey, keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().sinter(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sinter(keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().sinterstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sinterstore(destKey, keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().sismember(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sismember(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().smembers(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().smembers(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().smove(srcKey, destKey, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().smove(srcKey, destKey, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().spop(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().spop(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().srandmember(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().srandmember(key)));
				return null;
			}
			return getConnection().srandmember(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public List<byte[]> sRandMember(byte[] key, long count) {
		if(count < 0) {
			throw new UnsupportedOperationException("sRandMember with a negative count is not supported");
		}
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().srandmember(key, count), LettuceConverters.bytesSetToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().srandmember(key, count), LettuceConverters.bytesSetToBytesList()));
				return null;
			}
			return LettuceConverters.toBytesList(getConnection().srandmember(key, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long sRem(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().srem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().srem(key, values)));
				return null;
			}
			return getConnection().srem(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().sunion(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sunion(keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().sunionstore(destKey, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().sunionstore(destKey, keys)));
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
				pipeline(new LettuceResult(getAsyncConnection().zadd(key, score, value), LettuceConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zadd(key, score, value), LettuceConverters.longToBoolean()));
				return null;
			}
			return LettuceConverters.toBoolean(getConnection().zadd(key, score, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zAdd(byte[] key, Set<Tuple> tuples) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zadd(key,
						LettuceConverters.toObjects(tuples).toArray())));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zadd(key,
						LettuceConverters.toObjects(tuples).toArray())));
				return null;
			}
			return getConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zcard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zcard(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().zcount(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zcount(key, min, max)));
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
				pipeline(new LettuceResult(getAsyncConnection().zincrby(key, increment, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zincrby(key, increment, value)));
				return null;
			}
			return getConnection().zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zinterstore(destKey, storeArgs, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zinterstore(destKey, storeArgs, sets)));
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
				pipeline(new LettuceResult(getAsyncConnection().zinterstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zinterstore(destKey, sets)));
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
				pipeline(new LettuceResult(getAsyncConnection().zrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrangebyscore(key, min, max),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrangebyscore(key, min, max),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrangebyscoreWithScores(key, min, max),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrangebyscoreWithScores(key, min, max),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrangebyscoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrangebyscore(key, min, max, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrangebyscore(key, min, max, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrangebyscoreWithScores(key, min, max, offset, count),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrangebyscoreWithScores(key, min, max, offset, count),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrangebyscoreWithScores(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscore(key, max, min, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrangebyscore(key, max, min, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrevrangebyscore(key, max, min, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscore(key, max, min),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrangebyscore(key, max, min),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrevrangebyscore(key, max, min));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min, offset, count),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrangebyscoreWithScores(key, max, min, offset, count),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangebyscoreWithScores(key, max, min, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrangebyscoreWithScores(key, max, min),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangebyscoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrank(key, value)));
				return null;
			}
			return getConnection().zrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public Long zRem(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrem(key, values)));
				return null;
			}
			return getConnection().zrem(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zremrangebyrank(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zremrangebyrank(key, start, end)));
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
				pipeline(new LettuceResult(getAsyncConnection().zremrangebyscore(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zremrangebyscore(key, min, max)));
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
				pipeline(new LettuceResult(getAsyncConnection().zrevrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrevrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zrevrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zrevrank(key, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().zscore(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zscore(key, value)));
				return null;
			}
			return getConnection().zscore(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().zunionstore(destKey, storeArgs, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zunionstore(destKey, storeArgs, sets)));
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
				pipeline(new LettuceResult(getAsyncConnection().zunionstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zunionstore(destKey, sets)));
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
				pipeline(new LettuceResult(getAsyncConnection().hset(key, field, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hset(key, field, value)));
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
				pipeline(new LettuceResult(getAsyncConnection().hsetnx(key, field, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hsetnx(key, field, value)));
				return null;
			}
			return getConnection().hsetnx(key, field, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hDel(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().hdel(key, fields)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hdel(key, fields)));
				return null;
			}
			return getConnection().hdel(key, fields);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().hexists(key, field)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hexists(key, field)));
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
				pipeline(new LettuceResult(getAsyncConnection().hget(key, field)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hget(key, field)));
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
				pipeline(new LettuceResult(getAsyncConnection().hgetall(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hgetall(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().hincrby(key, field, delta)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hincrby(key, field, delta)));
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
				pipeline(new LettuceResult(getAsyncConnection().hincrbyfloat(key, field, delta)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hincrbyfloat(key, field, delta)));
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
				pipeline(new LettuceResult(getAsyncConnection().hkeys(key), LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hkeys(key), LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().hkeys(key));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().hlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hlen(key)));
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
				pipeline(new LettuceResult(getAsyncConnection().hmget(key, fields)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hmget(key, fields)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().hmset(key, tuple)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().hmset(key, tuple)));
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
				pipeline(new LettuceResult(getAsyncConnection().hvals(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().hvals(key)));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().scriptFlush()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().scriptFlush()));
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
				pipeline(new LettuceStatusResult(getAsyncConnection().scriptKill()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().scriptKill()));
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
				pipeline(new LettuceResult(getAsyncConnection().scriptLoad(script)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().scriptLoad(script)));
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
				pipeline(new LettuceResult(getAsyncConnection().scriptExists(scriptSha1)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().scriptExists(scriptSha1)));
				return null;
			}
			return getConnection().scriptExists(scriptSha1);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
			byte[][] args = extractScriptArgs(numKeys, keysAndArgs);

			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().eval(script, LettuceConverters.toScriptOutputType(returnType), keys, args)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().eval(script, LettuceConverters.toScriptOutputType(returnType), keys, args)));
				return null;
			}
			return getConnection().eval(script, LettuceConverters.toScriptOutputType(returnType), keys, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
			byte[][] args = extractScriptArgs(numKeys, keysAndArgs);

			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().evalsha(scriptSha1, LettuceConverters.toScriptOutputType(returnType),
						keys, args)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().evalsha(scriptSha1, LettuceConverters.toScriptOutputType(returnType),
						keys, args)));
				return null;
			}
			return getConnection().evalsha(scriptSha1, LettuceConverters.toScriptOutputType(returnType), keys, args);
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
				pipeline(new LettuceResult(getAsyncConnection().publish(channel, message)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().publish(channel, message)));
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

		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			subscription = new LettuceSubscription(listener, switchToPubSub());
			subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}


	public void subscribe(MessageListener listener, byte[]... channels) {
		checkSubscription();

		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			subscription = new LettuceSubscription(listener, switchToPubSub());
			subscription.subscribe(channels);

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/**
	 * Specifies if pipelined and transaction results should be converted to the expected data
	 * type. If false, results of {@link #closePipeline()} and {@link #exec()} will be of the
	 * type returned by the Lettuce driver
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
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
		return client.connectPubSub(CODEC);
	}

	private void pipeline(LettuceResult result) {
		if(isQueueing()) {
			transaction(result);
		}else {
			ppline.add(result);
		}
	}

	private void transaction(FutureResult<?> result) {
		txResults.add(result);
	}

	private RedisAsyncConnection<byte[], byte[]> getAsyncConnection() {
		if (isQueueing()) {
			return getAsyncDedicatedConnection();
		}
		if (asyncSharedConn != null) {
			return asyncSharedConn;
		}
		return getAsyncDedicatedConnection();
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getConnection() {
		if (isQueueing()) {
			return getDedicatedConnection();
		}
		if (sharedConn != null) {
			return sharedConn;
		}
		return getDedicatedConnection();
	}

	private RedisAsyncConnection<byte[], byte[]> getAsyncDedicatedConnection() {
		if(asyncDedicatedConn == null) {
			if(this.pool != null) {
				this.asyncDedicatedConn = pool.getResource();
			} else {
				this.asyncDedicatedConn = client.connectAsync(CODEC);
			}
		}
		return asyncDedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getDedicatedConnection() {
		if(dedicatedConn == null) {
			this.dedicatedConn = new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(getAsyncDedicatedConnection());
		}
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

	private byte[][] extractScriptKeys(int numKeys, byte[]... keysAndArgs) {
		if(numKeys > 0) {
			return Arrays.copyOfRange(keysAndArgs, 0,numKeys);
		}
		return new byte[0][0];
	}

	private byte[][] extractScriptArgs(int numKeys, byte[]... keysAndArgs) {
		if(keysAndArgs.length > numKeys) {
			return Arrays.copyOfRange(keysAndArgs, numKeys, keysAndArgs.length);
		}
		return new byte[0][0];
	}

	private ZStoreArgs zStoreArgs(Aggregate aggregate, int[] weights) {
		ZStoreArgs args = new ZStoreArgs();
		if (aggregate != null) {
			switch (aggregate) {
			case MIN:
				args.min();
				break;
			case MAX:
				args.max();
				break;
			default:
				args.sum();
				break;
			}
		}
		long[] lg = new long[weights.length];
		for (int i = 0; i < lg.length; i++) {
			lg[i] = (long) weights[i];
		}
		args.weights(lg);
		return args;
	}
}