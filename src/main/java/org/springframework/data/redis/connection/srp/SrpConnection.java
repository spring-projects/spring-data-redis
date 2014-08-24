/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;

import redis.Command;
import redis.client.RedisClient;
import redis.client.RedisClient.Pipeline;
import redis.client.RedisException;
import redis.reply.MultiBulkReply;
import redis.reply.Reply;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/spullara/redis-protocol">spullara Redis
 * Protocol</a> library.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public class SrpConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			SrpConverters.exceptionConverter());

	private static final Object[] EMPTY_PARAMS_ARRAY = new Object[0];
	private static final byte[] WITHSCORES = "WITHSCORES".getBytes(Charsets.UTF_8);
	private static final byte[] BY = "BY".getBytes(Charsets.UTF_8);
	private static final byte[] GET = "GET".getBytes(Charsets.UTF_8);
	private static final byte[] ALPHA = "ALPHA".getBytes(Charsets.UTF_8);
	private static final byte[] STORE = "STORE".getBytes(Charsets.UTF_8);

	private final RedisClient client;
	private final BlockingQueue<SrpConnection> queue;

	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean pipelineRequested = false;
	private Pipeline pipeline;
	private PipelineTracker callback;
	private PipelineTracker txTracker;
	private volatile SrpSubscription subscription;
	private boolean convertPipelineAndTxResults = true;

	@SuppressWarnings("rawtypes")
	private class PipelineTracker implements FutureCallback<Reply> {

		private final List<Object> results = Collections.synchronizedList(new ArrayList<Object>());
		private final Queue<FutureResult> futureResults = new LinkedList<FutureResult>();
		private boolean convertResults;

		public PipelineTracker(boolean convertResults) {
			this.convertResults = convertResults;
		}

		public void onSuccess(Reply result) {
			results.add(result.data());
		}

		public void onFailure(Throwable t) {
			results.add(t);
		}

		@SuppressWarnings("unchecked")
		public List<Object> complete() {
			int txResults = 0;
			List<ListenableFuture<? extends Reply>> futures = new ArrayList<ListenableFuture<? extends Reply>>();
			for (FutureResult future : futureResults) {
				if (future instanceof SrpTxResult) {
					txResults++;
				} else {
					ListenableFuture<? extends Reply> f = (ListenableFuture<? extends Reply>) future.getResultHolder();
					futures.add(f);
				}
			}
			try {
				Futures.successfulAsList(futures).get();
			} catch (Exception ex) {
				// ignore
			}
			if (futureResults.size() != results.size() + txResults) {
				throw new RedisPipelineException("Received a different number of results than expected. Expected: "
						+ futureResults.size(), results);
			}
			List<Object> convertedResults = new ArrayList<Object>();

			int i = 0;
			for (FutureResult future : futureResults) {
				if (future instanceof SrpTxResult) {
					PipelineTracker txTracker = ((SrpTxResult) future).getResultHolder();
					if (txTracker != null) {
						convertedResults.add(getPipelinedResults(txTracker, true));
					} else {
						convertedResults.add(null);
					}
				} else {
					Object result = results.get(i);
					if (result instanceof Exception || !convertResults) {
						convertedResults.add(result);
					} else if (!(future.isStatus())) {
						convertedResults.add(future.convert(result));
					}
					i++;
				}
			}
			return convertedResults;
		}

		public void addCommand(FutureResult result) {
			futureResults.add(result);
			if (!(result instanceof SrpTxResult) && result.getResultHolder() != null) {
				Futures.addCallback(((SrpGenericResult) result).getResultHolder(), this);
			}
		}

		public void close() {
			results.clear();
			futureResults.clear();
		}
	}

	@SuppressWarnings("rawtypes")
	private class SrpGenericResult extends FutureResult<ListenableFuture<? extends Reply>> {
		public SrpGenericResult(ListenableFuture<? extends Reply> resultHolder, Converter converter) {
			super(resultHolder, converter);
		}

		public SrpGenericResult(ListenableFuture<? extends Reply> resultHolder) {
			super(resultHolder);
		}

		@Override
		public Object get() {
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("rawtypes")
	private class SrpResult extends SrpGenericResult {
		public <T> SrpResult(ListenableFuture<? extends Reply<T>> resultHolder, Converter<T, ?> converter) {
			super(resultHolder, converter);
		}

		public SrpResult(ListenableFuture<? extends Reply> resultHolder) {
			super(resultHolder);
		}
	}

	private class SrpStatusResult extends SrpResult {
		@SuppressWarnings("rawtypes")
		public SrpStatusResult(ListenableFuture<? extends Reply> resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	private class SrpTxResult extends FutureResult<PipelineTracker> {
		public SrpTxResult(PipelineTracker txTracker) {
			super(txTracker);
		}

		public List<Object> get() {
			if (resultHolder == null) {
				return null;
			}
			return resultHolder.complete();
		}
	}

	SrpConnection(RedisClient client) {

		Assert.notNull(client);
		this.client = client;
		this.queue = new ArrayBlockingQueue<SrpConnection>(50);
	}

	public SrpConnection(String host, int port, BlockingQueue<SrpConnection> queue) {
		try {
			this.client = new RedisClient(host, port);
			this.queue = queue;
		} catch (IOException e) {
			throw new RedisConnectionFailureException("Could not connect", e);
		} catch (RedisException e) {
			throw new RedisConnectionFailureException("Could not connect", e);
		}
	}

	public SrpConnection(String host, int port, String password, BlockingQueue<SrpConnection> queue) {
		this(host, port, queue);
		try {
			this.client.auth(password);
		} catch (RedisException e) {
			throw new RedisConnectionFailureException("Could not connect", e);
		}
	}

	protected DataAccessException convertSrpAccessException(Exception ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	public Object execute(String command, byte[]... args) {
		Assert.hasText(command, "a valid command needs to be specified");
		try {
			String name = command.trim().toUpperCase();
			Command cmd = new Command(name.getBytes(Charsets.UTF_8), args);
			if (isPipelined()) {
				pipeline(new SrpResult(client.pipeline(name, cmd)));
				return null;
			}
			return client.execute(name, cmd).data();
		} catch (RedisException ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void close() throws DataAccessException {

		super.close();
		isClosed = true;
		queue.remove(this);

		if (subscription != null) {
			if (subscription.isAlive()) {
				subscription.doClose();
			}
			subscription = null;
		}

		try {
			client.close();
		} catch (IOException ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public boolean isClosed() {
		return isClosed;
	}

	public RedisClient getNativeConnection() {
		return client;
	}

	public boolean isQueueing() {
		return isMulti;
	}

	public boolean isPipelined() {
		return pipelineRequested || (txTracker != null);
	}

	public void openPipeline() {
		pipelineRequested = true;
		initPipeline();
	}

	public List<byte[]> sort(byte[] key, SortParameters params) {

		Object[] sort = sortParams(params);

		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.sort(key, sort), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList((Reply[]) client.sort(key, sort).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		Object[] sort = sortParams(params, sortKey);

		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sort(key, sort)));
				return null;
			}
			return ((Long) client.sort(key, sort).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.dbsize()));
				return null;
			}
			return client.dbsize().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.flushdb()));
				return;
			}
			client.flushdb();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.flushall()));
				return;
			}
			client.flushall();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.bgsave()));
				return;
			}
			client.bgsave();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void bgReWriteAof() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.bgrewriteaof()));
				return;
			}
			client.bgrewriteaof();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	/**
	 * @deprecated As of 1.3, use {@link #bgReWriteAof}.
	 */
	@Deprecated
	public void bgWriteAof() {
		bgReWriteAof();
	}

	public void save() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.save()));
				return;
			}
			client.save();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.config_get(param), SrpConverters.repliesToStringList()));
				return null;
			}
			return SrpConverters.toStringList(client.config_get(param).data().toString());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.info(null), SrpConverters.bytesToProperties()));
				return null;
			}
			return SrpConverters.toProperties(client.info(null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Properties info(String section) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.info(section), SrpConverters.bytesToProperties()));
				return null;
			}
			return SrpConverters.toProperties(client.info(section).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lastsave()));
				return null;
			}
			return client.lastsave().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.config_set(param, value)));
				return;
			}
			client.config_set(param, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.config_resetstat()));
				return;
			}
			client.config_resetstat();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void shutdown() {
		byte[] save = "SAVE".getBytes(Charsets.UTF_8);
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.shutdown(save, null)));
				return;
			}
			client.shutdown(save, null);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		byte[] save = option.name().getBytes(Charsets.UTF_8);
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.shutdown(save, null)));
				return;
			}
			client.shutdown(save, null);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}

	}

	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.echo(message)));
				return null;
			}
			return client.echo(message).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.ping()));
				return null;
			}
			return client.ping().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long del(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.del((Object[]) keys)));
				return null;
			}
			return client.del((Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void discard() {
		isMulti = false;
		try {
			// discard tracked futures
			txTracker = null;
			client.discard();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<Object> exec() {
		isMulti = false;
		try {
			Future<Boolean> exec = client.exec();
			// Need to wait on execution or subsequent non-pipelined calls may read exec results
			boolean resultsSet = exec.get();
			if (!resultsSet) {
				// This is the case where a nil MultiBulk Reply was returned b/c watched variable modified
				if (pipelineRequested) {
					pipeline(new SrpTxResult(null));
				}
				return null;
			}
			if (pipelineRequested) {
				pipeline(new SrpTxResult(txTracker));
				return null;
			}
			return closeTransaction();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		} finally {
			txTracker = null;
		}
	}

	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.exists(key), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.exists(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.expire(key, seconds), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.expire(key, seconds).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.expireat(key, unixTime), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.expireat(key, unixTime).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean pExpire(byte[] key, long millis) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.pexpire(key, millis), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.pexpire(key, millis).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.pexpireat(key, unixTimeInMillis), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.pexpireat(key, unixTimeInMillis).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> keys(byte[] pattern) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.keys(pattern), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.keys(pattern).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		initTxTracker();
		try {
			client.multi();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.persist(key), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.persist(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean move(byte[] key, int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.move(key, dbIndex), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.move(key, dbIndex).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.randomkey()));
				return null;
			}
			return client.randomkey().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void rename(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.rename(oldName, newName)));
				return;
			}
			client.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.renamenx(oldName, newName), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.renamenx(oldName, newName).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.select(dbIndex)));
				return;
			}
			client.select(dbIndex);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.ttl(key)));
				return null;
			}
			return client.ttl(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long pTtl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.pttl(key)));
				return null;
			}
			return client.pttl(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] dump(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.dump(key)));
				return null;
			}
			return client.dump(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.restore(key, ttlInMillis, serializedValue)));
				return;
			}
			client.restore(key, ttlInMillis, serializedValue);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public DataType type(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.type(key), SrpConverters.stringToDataType()));
				return null;
			}
			return SrpConverters.toDataType(client.type(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.unwatch()));
				return;
			}
			client.unwatch();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.watch((Object[]) keys)));
				return;
			} else {
				client.watch((Object[]) keys);
			}
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// String commands
	//

	public byte[] get(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.get(key)));
				return null;
			}

			return client.get(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void set(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.set(key, value)));
				return;
			}
			client.set(key, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.getset(key, value)));
				return null;
			}
			return client.getset(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long append(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.append(key, value)));
				return null;
			}
			return client.append(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> mGet(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.mget((Object[]) keys), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.mget((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.mset((Object[]) SrpConverters.toByteArrays(tuples))));
				return;
			}
			client.mset((Object[]) SrpConverters.toByteArrays(tuples));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.msetnx((Object[]) SrpConverters.toByteArrays(tuples)),
						SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.msetnx((Object[]) SrpConverters.toByteArrays(tuples)).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.setex(key, time, value)));
				return;
			}
			client.setex(key, time, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public void pSetEx(byte[] key, long milliseconds, byte[] value) {

		try {
			if (isPipelined()) {
				doPipelined(pipeline.psetex(key, milliseconds, value));
				return;
			}
			client.psetex(key, milliseconds, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.setnx(key, value), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.setnx(key, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] getRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.getrange(key, start, end)));
				return null;
			}
			return client.getrange(key, start, end).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long decr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.decr(key)));
				return null;
			}
			return client.decr(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long decrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.decrby(key, value)));
				return null;
			}
			return client.decrby(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long incr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.incr(key)));
				return null;
			}
			return client.incr(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long incrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.incrby(key, value)));
				return null;
			}
			return client.incrby(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Double incrBy(byte[] key, double value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.incrbyfloat(key, value), SrpConverters.bytesToDouble()));
				return null;
			}
			return SrpConverters.toDouble(client.incrbyfloat(key, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.getbit(key, offset), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.getbit(key, offset).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.setbit(key, offset, SrpConverters.toBit(value)),
						SrpConverters.longToBooleanConverter()));
				return null;
			}
			return SrpConverters.toBoolean(client.setbit(key, offset, SrpConverters.toBit(value)));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.setrange(key, start, value)));
				return;
			}
			client.setrange(key, start, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.strlen(key)));
				return null;
			}
			return client.strlen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long bitCount(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.bitcount(key, 0, -1)));
				return null;
			}
			return client.bitcount(key, 0, -1).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long bitCount(byte[] key, long begin, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.bitcount(key, begin, end)));
				return null;
			}
			return client.bitcount(key, begin, end).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
		if (op == BitOperation.NOT && keys.length > 1) {
			throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
		}
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.bitop(SrpConverters.toBytes(op), destination, (Object[]) keys)));
				return null;
			}
			return client.bitop(SrpConverters.toBytes(op), destination, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// List commands
	//

	public Long lPush(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lpush(key, (Object[]) values)));
				return null;
			}
			return client.lpush(key, (Object[]) values).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long rPush(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.rpush(key, (Object[]) values)));
				return null;
			}
			return client.rpush(key, (Object[]) values).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		Object[] args = popArgs(timeout, keys);

		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.blpop(args), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.blpop(args).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		Object[] args = popArgs(timeout, keys);
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.brpop(args), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.brpop(args).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lindex(key, index)));
				return null;
			}
			return client.lindex(key, index).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.linsert(key, SrpConverters.toBytes(where), pivot, value)));
				return null;
			}
			return client.linsert(key, SrpConverters.toBytes(where), pivot, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.llen(key)));
				return null;
			}
			return client.llen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] lPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lpop(key)));
				return null;
			}
			return client.lpop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lrange(key, start, end), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.lrange(key, start, end).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lrem(key, count, value)));
				return null;
			}
			return client.lrem(key, count, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.lset(key, index, value)));
				return;
			}
			client.lset(key, index, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.ltrim(key, start, end)));
				return;
			}
			client.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] rPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.rpop(key)));
				return null;
			}
			return client.rpop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.rpoplpush(srcKey, dstKey)));
				return null;
			}
			return client.rpoplpush(srcKey, dstKey).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.brpoplpush(srcKey, dstKey, timeout)));
				return null;
			}
			return (byte[]) client.brpoplpush(srcKey, dstKey, timeout).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.lpushx(key, value)));
				return null;
			}
			return client.lpushx(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.rpushx(key, value)));
				return null;
			}
			return client.rpushx(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// Set commands
	//

	public Long sAdd(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sadd(key, (Object[]) values)));
				return null;
			}
			return client.sadd(key, (Object[]) values).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.scard(key)));
				return null;
			}
			return client.scard(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sdiff((Object[]) keys), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.sdiff((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sdiffstore(destKey, (Object[]) keys)));
				return null;
			}
			return client.sdiffstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sinter((Object[]) keys), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.sinter((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sinterstore(destKey, (Object[]) keys)));
				return null;
			}
			return client.sinterstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sismember(key, value), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.sismember(key, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.smembers(key), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.smembers(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.smove(srcKey, destKey, value), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.smove(srcKey, destKey, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.spop(key)));
				return null;
			}
			return client.spop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.srandmember(key, null)));
				return null;
			}
			return (byte[]) client.srandmember(key, null).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> sRandMember(byte[] key, long count) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.srandmember(key, count), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(((MultiBulkReply) client.srandmember(key, count)).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sRem(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.srem(key, (Object[]) values)));
				return null;
			}
			return client.srem(key, (Object[]) values).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sunion((Object[]) keys), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.sunion((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.sunionstore(destKey, (Object[]) keys)));
				return null;
			}
			return client.sunionstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// ZSet commands
	//

	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zadd(new Object[] { key, score, value }), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.zadd(new Object[] { key, score, value }).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zAdd(byte[] key, Set<Tuple> tuples) {
		try {
			List<Object> args = new ArrayList<Object>();
			args.add(key);
			args.addAll(SrpConverters.toObjects(tuples));
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zadd(args.toArray())));
				return null;
			}
			return client.zadd(args.toArray()).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zcard(key)));
				return null;
			}
			return client.zcard(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zcount(key, min, max)));
				return null;
			}
			return client.zcount(key, min, max).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zincrby(key, increment, value), SrpConverters.bytesToDouble()));
				return null;
			}
			return SrpConverters.toDouble(client.zincrby(key, increment, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, double[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Long zInterStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zinterstore(destKey, sets.length, (Object[]) sets)));
				return null;
			}
			return client.zinterstore(destKey, sets.length, (Object[]) sets).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrange(key, start, end, null), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrange(key, start, end, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrange(key, start, end, WITHSCORES), SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrange(key, start, end, WITHSCORES).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrangebyscore(key, min, max, null, EMPTY_PARAMS_ARRAY),
						SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrangebyscore(key, min, max, null, EMPTY_PARAMS_ARRAY).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrangebyscore(key, min, max, WITHSCORES, EMPTY_PARAMS_ARRAY),
						SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrangebyscore(key, min, max, WITHSCORES, EMPTY_PARAMS_ARRAY).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrange(key, start, end, WITHSCORES), SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrevrange(key, start, end, WITHSCORES).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			Object[] limit = limitParams(offset, count);
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrangebyscore(key, min, max, null, limit), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrangebyscore(key, min, max, null, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			Object[] limit = limitParams(offset, count);
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrangebyscore(key, min, max, WITHSCORES, limit),
						SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrangebyscore(key, min, max, WITHSCORES, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			Object[] limit = limitParams(offset, count);
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrangebyscore(key, max, min, null, limit), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrevrangebyscore(key, max, min, null, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrangebyscore(key, max, min, null, EMPTY_PARAMS_ARRAY),
						SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrevrangebyscore(key, max, min, null, EMPTY_PARAMS_ARRAY).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			Object[] limit = limitParams(offset, count);
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrangebyscore(key, max, min, WITHSCORES, limit),
						SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrevrangebyscore(key, max, min, WITHSCORES, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrangebyscore(key, max, min, WITHSCORES, EMPTY_PARAMS_ARRAY),
						SrpConverters.repliesToTupleSet()));
				return null;
			}
			return SrpConverters.toTupleSet(client.zrevrangebyscore(key, max, min, WITHSCORES, EMPTY_PARAMS_ARRAY).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrank(key, value)));
				return null;
			}
			return (Long) client.zrank(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zRem(byte[] key, byte[]... values) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrem(key, (Object[]) values)));
				return null;
			}
			return client.zrem(key, (Object[]) values).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zremrangebyrank(key, start, end)));
				return null;
			}
			return client.zremrangebyrank(key, start, end).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zremrangebyscore(key, min, max)));
				return null;
			}
			return client.zremrangebyscore(key, min, max).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrange(key, start, end, null), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.zrevrange(key, start, end, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zrevrank(key, value)));
				return null;
			}
			return (Long) client.zrevrank(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Double zScore(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zscore(key, value), SrpConverters.bytesToDouble()));
				return null;
			}
			return SrpConverters.toDouble(client.zscore(key, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long zUnionStore(byte[] destKey, Aggregate aggregate, double[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.zunionstore(destKey, sets.length, (Object[]) sets)));
				return null;
			}
			return client.zunionstore(destKey, sets.length, (Object[]) sets).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// Hash commands
	//

	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hset(key, field, value), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.hset(key, field, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hsetnx(key, field, value), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.hsetnx(key, field, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long hDel(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hdel(key, (Object[]) fields)));
				return null;
			}
			return client.hdel(key, (Object[]) fields).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hexists(key, field), SrpConverters.longToBoolean()));
				return null;
			}
			return SrpConverters.toBoolean(client.hexists(key, field).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public byte[] hGet(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hget(key, field)));
				return null;
			}
			return client.hget(key, field).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hgetall(key), SrpConverters.repliesToBytesMap()));
				return null;
			}
			return SrpConverters.toBytesMap(client.hgetall(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hincrby(key, field, delta)));
				return null;
			}
			return client.hincrby(key, field, delta).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hincrbyfloat(key, field, delta), SrpConverters.bytesToDouble()));
				return null;
			}
			return SrpConverters.toDouble(client.hincrbyfloat(key, field, delta).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hkeys(key), SrpConverters.repliesToBytesSet()));
				return null;
			}
			return SrpConverters.toBytesSet(client.hkeys(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hlen(key)));
				return null;
			}
			return client.hlen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hmget(key, (Object[]) fields), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.hmget(key, (Object[]) fields).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.hmset(key, (Object[]) SrpConverters.toByteArrays(tuple))));
				return;
			}
			client.hmset(key, (Object[]) SrpConverters.toByteArrays(tuple));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<byte[]> hVals(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.hvals(key), SrpConverters.repliesToBytesList()));
				return null;
			}
			return SrpConverters.toBytesList(client.hvals(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// Scripting commands
	//

	public void scriptFlush() {
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.script_flush()));
				return;
			}
			client.script_flush();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void scriptKill() {
		if (isQueueing()) {
			throw new UnsupportedOperationException("Script kill not permitted in a transaction");
		}
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.script_kill()));
				return;
			}
			client.script_kill();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public String scriptLoad(byte[] script) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.script_load(script), SrpConverters.bytesToString()));
				return null;
			}
			return SrpConverters.toString((byte[]) client.script_load(script).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public List<Boolean> scriptExists(String... scriptSha1) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.script_exists((Object[]) scriptSha1),
						SrpConverters.repliesToBooleanList()));
				return null;
			}
			return SrpConverters.toBooleanList(((MultiBulkReply) client.script_exists_((Object[]) scriptSha1)).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.eval(script, numKeys, (Object[]) keysAndArgs),
						new SrpScriptReturnConverter(returnType)));
				return null;
			}
			return (T) new SrpScriptReturnConverter(returnType).convert(client.eval(script, numKeys, (Object[]) keysAndArgs)
					.data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.evalsha(scriptSha1, numKeys, (Object[]) keysAndArgs),
						new SrpScriptReturnConverter(returnType)));
				return null;
			}
			return (T) new SrpScriptReturnConverter(returnType).convert(client.evalsha(scriptSha1, numKeys,
					(Object[]) keysAndArgs).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	public Long publish(byte[] channel, byte[] message) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			if (isPipelined()) {
				pipeline(new SrpResult(pipeline.publish(channel, message)));
				return null;
			}
			return client.publish(channel, message).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
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
			subscription = new SrpSubscription(listener, client);
			subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void subscribe(MessageListener listener, byte[]... channels) {
		checkSubscription();

		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			subscription = new SrpSubscription(listener, client);
			subscription.subscribe(channels);

		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Lettuce driver
	 * 
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline results
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

	@SuppressWarnings("rawtypes")
	private void doPipelined(ListenableFuture<Reply> listenableFuture) {
		pipeline(new SrpStatusResult(listenableFuture));
	}

	// processing method that adds a listener to the future in order to track down the results and close the pipeline
	private void pipeline(FutureResult<?> future) {
		if (isQueueing()) {
			txTracker.addCommand(future);
		} else {
			callback.addCommand(future);
		}
	}

	private void initPipeline() {
		if (pipeline == null) {
			callback = new PipelineTracker(convertPipelineAndTxResults);
			pipeline = client.pipeline();
		}
	}

	private void initTxTracker() {
		if (txTracker == null) {
			txTracker = new PipelineTracker(convertPipelineAndTxResults);
		}
		initPipeline();
	}

	public List<Object> closePipeline() {
		pipelineRequested = false;
		List<Object> results = Collections.emptyList();
		if (pipeline != null) {
			pipeline = null;
			results = getPipelinedResults(callback, true);
			callback.close();
			callback = null;
		}
		return results;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {

		if (isPipelined()) {
			pipeline(new SrpGenericResult(pipeline.time(), SrpConverters.repliesToTimeAsLong()));
			return null;
		}

		MultiBulkReply reply = this.client.time();
		Assert.notNull(reply, "Received invalid result from server. MultiBulkReply must not be empty.");

		return SrpConverters.toTimeAsLong(reply.data());
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");

		String client = String.format("%s:%s", host, port);
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.client_kill(client)));
				return;
			}

			this.client.client_kill(client);
		} catch (Exception e) {
			throw convertSrpAccessException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(byte[])
	 */
	@Override
	public void setClientName(byte[] name) {

		try {

			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.client_setname(name)));
				return;
			}

			this.client.client_setname(name);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	*/
	@Override
	public void slaveOf(String host, int port) {

		Assert.hasText(host, "Host must not be null for 'SLAVEOF' command.");
		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.slaveof(host, port)));
				return;
			}
			client.slaveof(host, port);
		} catch (Exception e) {
			throw convertSrpAccessException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {

		try {

			if (isPipelined()) {
				pipeline(new SrpGenericResult(pipeline.client_getname(), SrpConverters.replyToString()));
				return null;
			}

			return SrpConverters.toString(client.client_getname());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	@Override
	public List<RedisClientInfo> getClientList() {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			pipeline(new SrpGenericResult(pipeline.client_list(), SrpConverters.replyToListOfRedisClientInfo()));
			return null;
		}

		return SrpConverters.toListOfRedisClientInformation(this.client.client_list());
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {

		try {
			if (isPipelined()) {
				pipeline(new SrpStatusResult(pipeline.slaveof("NO", "ONE")));
				return;
			}
			client.slaveof("NO", "ONE");
		} catch (Exception e) {
			throw convertSrpAccessException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		throw new UnsupportedOperationException("'SCAN' command is not supported for Srp.");
	}

	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'ZSCAN' command is not supported for Srp.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'SSCAN' command is not supported for Srp.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hscan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'HSCAN' command is not supported for Srp.");
	}

	private List<Object> closeTransaction() {
		List<Object> results = Collections.emptyList();
		if (txTracker != null) {
			results = getPipelinedResults(txTracker, false);
			txTracker.close();
			txTracker = null;
		}
		return results;
	}

	private List<Object> getPipelinedResults(PipelineTracker tracker, boolean throwPipelineException) {
		List<Object> execute = new ArrayList<Object>(tracker.complete());
		if (execute != null && !execute.isEmpty()) {
			Exception cause = null;
			for (int i = 0; i < execute.size(); i++) {
				Object object = execute.get(i);
				if (object instanceof Exception) {
					DataAccessException dataAccessException = convertSrpAccessException((Exception) object);
					if (cause == null) {
						cause = dataAccessException;
					}
					execute.set(i, dataAccessException);
				}
			}
			if (cause != null) {
				if (throwPipelineException) {
					throw new RedisPipelineException(cause, execute);
				} else {
					throw convertSrpAccessException(cause);
				}
			}

			return execute;
		}
		return Collections.emptyList();
	}

	private Object[] popArgs(int timeout, byte[]... keys) {
		int length = (keys != null ? keys.length + 1 : 1);
		Object[] args = new Object[length];
		if (keys != null) {
			for (int i = 0; i < keys.length; i++) {
				args[i] = keys[i];
			}
		}
		args[length - 1] = String.valueOf(timeout).getBytes();
		return args;
	}

	private Object[] limitParams(long offset, long count) {
		return new Object[] { "LIMIT".getBytes(Charsets.UTF_8), String.valueOf(offset).getBytes(Charsets.UTF_8),
				String.valueOf(count).getBytes(Charsets.UTF_8) };
	}

	private byte[] limit(long offset, long count) {
		return ("LIMIT " + offset + " " + count).getBytes(Charsets.UTF_8);
	}

	private Object[] sortParams(SortParameters params) {
		return sortParams(params, null);
	}

	private Object[] sortParams(SortParameters params, byte[] sortKey) {
		List<byte[]> arrays = new ArrayList<byte[]>();
		if (params != null) {
			if (params.getByPattern() != null) {
				arrays.add(BY);
				arrays.add(params.getByPattern());
			}
			if (params.getLimit() != null) {
				arrays.add(limit(params.getLimit().getStart(), params.getLimit().getCount()));
			}
			if (params.getGetPattern() != null) {
				byte[][] pattern = params.getGetPattern();
				for (byte[] bs : pattern) {
					arrays.add(GET);
					arrays.add(bs);
				}
			}
			if (params.getOrder() != null) {
				arrays.add(params.getOrder().name().getBytes(Charsets.UTF_8));
			}
			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				arrays.add(ALPHA);
			}
		}
		if (sortKey != null) {
			arrays.add(STORE);
			arrays.add(sortKey);
		}
		return arrays.toArray();
	}

}
