/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static com.lambdaworks.redis.protocol.CommandType.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.BeanUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.RedisCommand;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.KeyScanCursor;
import com.lambdaworks.redis.LettuceFutures;
import com.lambdaworks.redis.MapScanCursor;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisAsyncConnectionImpl;
import com.lambdaworks.redis.RedisChannelHandler;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisClusterConnection;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisSentinelAsyncConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ScoredValueScanCursor;
import com.lambdaworks.redis.SortArgs;
import com.lambdaworks.redis.ValueScanCursor;
import com.lambdaworks.redis.ZStoreArgs;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.output.BooleanOutput;
import com.lambdaworks.redis.output.ByteArrayOutput;
import com.lambdaworks.redis.output.DateOutput;
import com.lambdaworks.redis.output.DoubleOutput;
import com.lambdaworks.redis.output.IntegerOutput;
import com.lambdaworks.redis.output.KeyListOutput;
import com.lambdaworks.redis.output.KeyValueOutput;
import com.lambdaworks.redis.output.MapOutput;
import com.lambdaworks.redis.output.MultiOutput;
import com.lambdaworks.redis.output.StatusOutput;
import com.lambdaworks.redis.output.ValueListOutput;
import com.lambdaworks.redis.output.ValueOutput;
import com.lambdaworks.redis.output.ValueSetOutput;
import com.lambdaworks.redis.protocol.Command;
import com.lambdaworks.redis.protocol.CommandArgs;
import com.lambdaworks.redis.protocol.CommandOutput;
import com.lambdaworks.redis.protocol.CommandType;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/mp911de/lettuce">Lettuce</a> Redis
 * client.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 */
public class LettuceConnection extends AbstractRedisConnection {

	static final RedisCodec<byte[], byte[]> CODEC = new BytesRedisCodec();

	private static final Method SYNC_HANDLER;
	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			LettuceConverters.exceptionConverter());
	private static final TypeHints typeHints = new TypeHints();

	private final int defaultDbIndex;
	private int dbIndex;

	static {
		SYNC_HANDLER = ReflectionUtils.findMethod(AbstractRedisClient.class, "syncHandler", RedisChannelHandler.class,
				Class[].class);
		ReflectionUtils.makeAccessible(SYNC_HANDLER);
	}

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
	private boolean convertPipelineAndTxResults = true;

	@SuppressWarnings("rawtypes")
	private class LettuceResult extends FutureResult<Command<?, ?, ?>> {
		public <T> LettuceResult(Future<T> resultHolder, Converter<T, ?> converter) {
			super((Command) resultHolder, converter);
		}

		public LettuceResult(Future resultHolder) {
			super((Command) resultHolder);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			try {
				if (convertPipelineAndTxResults && converter != null) {
					return converter.convert(resultHolder.get());
				}
				return resultHolder.get();
			} catch (ExecutionException e) {
				throw EXCEPTION_TRANSLATION.translate(e);
			}
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
		public LettuceTxResult(Object resultHolder, Converter<?, ?> converter) {
			super(resultHolder, converter);
		}

		public LettuceTxResult(Object resultHolder) {
			super(resultHolder);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			if (convertPipelineAndTxResults && converter != null) {
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
			if (execResults.isEmpty()) {
				return null;
			}
			return super.convert(execResults);
		}
	}

	private class LettuceEvalResultsConverter<T> implements Converter<Object, T> {
		private ReturnType returnType;

		public LettuceEvalResultsConverter(ReturnType returnType) {
			this.returnType = returnType;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public T convert(Object source) {
			if (returnType == ReturnType.MULTI) {
				List resultList = (List) source;
				for (Object obj : resultList) {
					if (obj instanceof Exception) {
						throw convertLettuceAccessException((Exception) obj);
					}
				}
			}
			return (T) source;
		}
	}

	/**
	 * Instantiates a new lettuce connection.
	 * 
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when instantiating a native connection
	 */
	public LettuceConnection(long timeout, RedisClient client) {
		this(null, timeout, client, null);
	}

	/**
	 * Instantiates a new lettuce connection.
	 * 
	 * @param timeout The connection timeout (in milliseconds) * @param client The {@link RedisClient} to use when
	 *          instantiating a pub/sub connection
	 * @param pool The connection pool to use for all other native connections
	 */
	public LettuceConnection(long timeout, RedisClient client, LettucePool pool) {
		this(null, timeout, client, pool);
	}

	/**
	 * Instantiates a new lettuce connection.
	 * 
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Will not be used
	 *          for transactions or blocking operations
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub, blocking, and tx connections
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection, long timeout,
			RedisClient client) {
		this(sharedConnection, timeout, client, null);
	}

	/**
	 * Instantiates a new lettuce connection.
	 * 
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub connections
	 * @param pool The connection pool to use for blocking and tx operations
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection, long timeout,
			RedisClient client, LettucePool pool) {

		this(sharedConnection, timeout, client, pool, 0);
	}

	/**
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub connections.
	 * @param pool The connection pool to use for blocking and tx operations.
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 1.7
	 */
	public LettuceConnection(com.lambdaworks.redis.RedisAsyncConnection<byte[], byte[]> sharedConnection, long timeout,
			RedisClient client, LettucePool pool, int defaultDbIndex) {

		this.asyncSharedConn = sharedConnection;
		this.timeout = timeout;
		this.client = client;
		this.sharedConn = sharedConnection != null ? syncConnection(asyncSharedConn) : null;
		this.pool = pool;
		this.defaultDbIndex = defaultDbIndex;
		this.dbIndex = this.defaultDbIndex;
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {

		DataAccessException exception = EXCEPTION_TRANSLATION.translate(ex);

		if (exception instanceof RedisConnectionFailureException) {
			broken = true;
		}
		return exception;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object await(com.lambdaworks.redis.protocol.RedisCommand cmd) {
		if (isMulti && cmd instanceof Command && ((Command) cmd).isMulti()) {
			return null;
		}
		return LettuceFutures.await(cmd, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public Object execute(String command, byte[]... args) {
		return execute(command, null, args);
	}

	/**
	 * 'Native' or 'raw' execution of the given command along-side the given arguments.
	 * 
	 * @see RedisCommands#execute(String, byte[]...)
	 * @param command Command to execute
	 * @param commandOutputTypeHint Type of Output to use, may be (may be {@literal null}).
	 * @param args Possible command arguments (may be {@literal null})
	 * @return execution result.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object execute(String command, CommandOutput commandOutputTypeHint, byte[]... args) {

		Assert.hasText(command, "a valid command needs to be specified");
		try {
			String name = command.trim().toUpperCase();
			CommandType commandType = CommandType.valueOf(name);

			validateCommandIfRunningInTransactionMode(commandType, args);

			CommandArgs<byte[], byte[]> cmdArg = new CommandArgs<byte[], byte[]>(CODEC);
			if (!ObjectUtils.isEmpty(args)) {
				cmdArg.addKeys(args);
			}

			RedisAsyncConnectionImpl<byte[], byte[]> connectionImpl = (RedisAsyncConnectionImpl<byte[], byte[]>) getAsyncConnection();

			CommandOutput expectedOutput = commandOutputTypeHint != null ? commandOutputTypeHint : typeHints
					.getTypeHint(commandType);
			Command cmd = new Command(commandType, expectedOutput, cmdArg);
			if (isPipelined()) {

				pipeline(new LettuceResult(connectionImpl.dispatch(cmd)));
				return null;
			} else if (isQueueing()) {

				transaction(new LettuceTxResult(connectionImpl.dispatch(cmd)));
				return null;
			} else {
				return await(connectionImpl.dispatch(cmd));
			}
		} catch (RedisException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private void returnDedicatedAsyncConnection() {

		if (pool != null) {

			if (!broken) {
				pool.returnResource(this.asyncDedicatedConn);
			} else {
				pool.returnBrokenResource(this.asyncDedicatedConn);
			}
			this.asyncDedicatedConn = null;

		} else {

			try {
				asyncDedicatedConn.close();
			} catch (RuntimeException ex) {
				throw convertLettuceAccessException(ex);
			}
		}
	}

	public void close() throws DataAccessException {
		super.close();

		isClosed = true;

		if (asyncDedicatedConn != null) {
			returnDedicatedAsyncConnection();
		}

		if (subscription != null) {
			if (subscription.isAlive()) {
				subscription.doClose();
			}
			subscription = null;
		}

		this.dbIndex = defaultDbIndex;
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
			for (LettuceResult result : ppline) {
				futures.add(result.getResultHolder());
			}
			boolean done = LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS,
					futures.toArray(new Command[futures.size()]));
			List<Object> results = new ArrayList<Object>(futures.size());

			Exception problem = null;

			if (done) {
				for (LettuceResult result : ppline) {
					if (result.getResultHolder().getOutput().hasError()) {
						Exception err = new InvalidDataAccessApiUsageException(result.getResultHolder().getOutput().getError());
						// remember only the first error
						if (problem == null) {
							problem = err;
						}
						results.add(err);
					} else if (!convertPipelineAndTxResults || !(result.isStatus())) {
						try {
							results.add(result.get());
						} catch (DataAccessException e) {
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

	public void bgReWriteAof() {
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

		boolean save = ShutdownOption.SAVE.equals(option);
		try {
			if (isPipelined()) {
				getAsyncConnection().shutdown(save);
				return;
			}
			getConnection().shutdown(save);
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
				pipeline(new LettuceResult(getAsyncDedicatedConnection().exec(), new LettuceTransactionResultConverter(
						new LinkedList<FutureResult<?>>(txResults), LettuceConverters.exceptionConverter())));
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

		if (asyncSharedConn != null) {
			throw new UnsupportedOperationException("Selecting a new database not supported due to shared connection. "
					+ "Use separate ConnectionFactorys to work with multiple databases");
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException("Lettuce blocks for #select");
		}
		try {

			this.dbIndex = dbIndex;
			if (isQueueing()) {
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
		if (isQueueing()) {
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
			if (isQueueing()) {
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
			if (isQueueing()) {
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

	/**
	 * @since 1.3
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public void pSetEx(byte[] key, long milliseconds, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().psetex(key, milliseconds, value)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(getConnection().psetex(key, milliseconds, value)));
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

	public Boolean setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().setbit(key, offset, LettuceConverters.toInt(value)),
						LettuceConverters.longToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().setbit(key, offset, LettuceConverters.toInt(value)),
						LettuceConverters.longToBooleanConverter()));
				return null;
			}
			return LettuceConverters.longToBooleanConverter().convert(
					getConnection().setbit(key, offset, LettuceConverters.toInt(value)));
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
				pipeline(new LettuceResult(getAsyncConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().linsert(key, LettuceConverters.toBoolean(where), pivot, value)));
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
		try {
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().srandmember(key, count),
						LettuceConverters.bytesSetToBytesList()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().srandmember(key, count),
						LettuceConverters.bytesSetToBytesList()));
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
				pipeline(new LettuceResult(getAsyncConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray())));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray())));
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

	@Override
	public Long zCount(byte[] key, double min, double max) {
		return zCount(key, new Range().gte(min).lte(max));
	}

	@Override
	public Long zCount(byte[] key, Range range) {

		Assert.notNull(range, "Range for ZCOUNT must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

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

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return zRangeByScore(key, new Range().gte(min).lte(max));
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range) {
		return zRangeByScore(key, range, null);
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(new LettuceResult(getAsyncConnection().zrangebyscore(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().zrangebyscore(key, min, max),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit != null) {
					transaction(new LettuceTxResult(getConnection().zrangebyscore(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(new LettuceTxResult(getConnection().zrangebyscore(key, min, max),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (limit != null) {
				return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max, limit.getOffset(),
						limit.getCount()));
			}
			return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRangeByScoreWithScores(key, new Range().gte(min).lte(max));
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
		return zRangeByScoreWithScores(key, range, null);
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(new LettuceResult(getAsyncConnection().zrangebyscoreWithScores(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.scoredValuesToTupleSet()));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().zrangebyscoreWithScores(key, min, max),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit != null) {
					transaction(new LettuceTxResult(getConnection().zrangebyscoreWithScores(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.scoredValuesToTupleSet()));
				} else {
					transaction(new LettuceTxResult(getConnection().zrangebyscoreWithScores(key, min, max),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (limit != null) {
				return LettuceConverters.toTupleSet(getConnection().zrangebyscoreWithScores(key, min, max, limit.getOffset(),
						limit.getCount()));
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

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {

		return zRangeByScore(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		return zRangeByScoreWithScores(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScore(key, new Range().gte(min).lte(max), new Limit().offset(Long.valueOf(offset).intValue())
				.count(Long.valueOf(count).intValue()));
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
		return zRevRangeByScore(key, range, null);
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscore(key, max, min, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscore(key, max, min),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit != null) {
					transaction(new LettuceTxResult(getConnection().zrevrangebyscore(key, max, min, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(new LettuceTxResult(getConnection().zrevrangebyscore(key, max, min),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (limit != null) {
				return LettuceConverters.toBytesSet(getConnection().zrevrangebyscore(key, max, min, limit.getOffset(),
						limit.getCount()));
			}
			return LettuceConverters.toBytesSet(getConnection().zrevrangebyscore(key, max, min));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return zRevRangeByScore(key, new Range().gte(min).lte(max));
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
		return zRevRangeByScoreWithScores(key, range, null);
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min, limit.getOffset(),
							limit.getCount()), LettuceConverters.scoredValuesToTupleSet()));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key, max, min),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit != null) {
					transaction(new LettuceTxResult(getConnection().zrevrangebyscoreWithScores(key, max, min, limit.getOffset(),
							limit.getCount()), LettuceConverters.scoredValuesToTupleSet()));
				} else {
					transaction(new LettuceTxResult(getConnection().zrevrangebyscoreWithScores(key, max, min),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (limit != null) {
				return LettuceConverters.toTupleSet(getConnection().zrevrangebyscoreWithScores(key, max, min,
						limit.getOffset(), limit.getCount()));
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangebyscoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max));
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

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		return zRemRangeByScore(key, new Range().gte(min).lte(max));
	}

	@Override
	public Long zRemRangeByScore(byte[] key, Range range) {

		Assert.notNull(range, "Range for ZREMRANGEBYSCORE must not be null!");

		String min = LettuceConverters.boundaryToStringForZRange(range.getMin(), "-inf");
		String max = LettuceConverters.boundaryToStringForZRange(range.getMax(), "+inf");

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
		if (isQueueing()) {
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
			String convertedScript = LettuceConverters.toString(script);
			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().eval(convertedScript,
						LettuceConverters.toScriptOutputType(returnType), keys, args), new LettuceEvalResultsConverter<T>(
						returnType)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().eval(convertedScript,
						LettuceConverters.toScriptOutputType(returnType), keys, args), new LettuceEvalResultsConverter<T>(
						returnType)));
				return null;
			}
			return new LettuceEvalResultsConverter<T>(returnType).convert(getConnection().eval(convertedScript,
					LettuceConverters.toScriptOutputType(returnType), keys, args));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		try {
			byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
			byte[][] args = extractScriptArgs(numKeys, keysAndArgs);

			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().evalsha(scriptSha1,
						LettuceConverters.toScriptOutputType(returnType), keys, args), new LettuceEvalResultsConverter<T>(
						returnType)));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().evalsha(scriptSha1,
						LettuceConverters.toScriptOutputType(returnType), keys, args), new LettuceEvalResultsConverter<T>(
						returnType)));
				return null;
			}
			return new LettuceEvalResultsConverter<T>(returnType).convert(getConnection().evalsha(scriptSha1,
					LettuceConverters.toScriptOutputType(returnType), keys, args));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return evalSha(LettuceConverters.toString(scriptSha1), returnType, numKeys, keysAndArgs);
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {

		try {

			if (isPipelined()) {
				pipeline(new LettuceResult(getAsyncConnection().time(), LettuceConverters.toTimeConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().time(), LettuceConverters.toTimeConverter()));
				return null;
			}

			return LettuceConverters.toTimeConverter().convert(getConnection().time());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");

		String client = String.format("%s:%s", host, port);
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().clientKill(client)));
				return;
			}
			getConnection().clientKill(client);
		} catch (Exception e) {
			convertLettuceAccessException(e);
		}
	}

	@Override
	public void setClientName(byte[] name) {

		if (isQueueing()) {
			pipeline(new LettuceStatusResult(getAsyncConnection().clientSetname(name)));
			return;
		}
		if (isQueueing()) {
			transaction(new LettuceTxResult(getConnection().clientSetname(name)));
			return;
		}

		getAsyncConnection().clientSetname(name);
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
				pipeline(new LettuceStatusResult(getAsyncConnection().slaveof(host, port)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().slaveof(host, port)));
				return;
			}
			getConnection().slaveof(host, port);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(new LettuceResult(getAsyncConnection().clientGetname(), LettuceConverters.bytesToString()));
				return null;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().clientGetname(), LettuceConverters.bytesToString()));
				return null;
			}

			return LettuceConverters.toString(getConnection().clientGetname());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public List<RedisClientInfo> getClientList() {

		if (isPipelined()) {
			throw new UnsupportedOperationException("Cannot be called in pipeline mode.");
		}
		if (isQueueing()) {
			transaction(new LettuceTxResult(getAsyncConnection().clientList(),
					LettuceConverters.stringToRedisClientListConverter()));
			return null;
		}

		return LettuceConverters.toListOfRedisClientInformation(getConnection().clientList());
	}

	/*
		* @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {

		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(getAsyncConnection().slaveofNoOne()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxResult(getConnection().slaveofNoOne()));
				return;
			}
			getConnection().slaveofNoOne();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/**
	 * @since 1.4
	 * @return
	 */
	public Cursor<byte[]> scan() {
		return scan(0, ScanOptions.NONE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	public Cursor<byte[]> scan(ScanOptions options) {
		return scan(0, options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@SuppressWarnings("unchecked")
			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'SCAN' cannot be called in pipeline / transaction mode.");
				}

				com.lambdaworks.redis.ScanCursor scanCursor = getScanCursor(cursorId);
				ScanArgs scanArgs = getScanArgs(options);

				if (isPipelined()) {
					pipeline(new LettuceResult(getAsyncConnection().scan(scanCursor, scanArgs)));
					return null;
				}

				if (isQueueing()) {
					transaction(new LettuceTxResult(getAsyncConnection().scan(scanCursor, scanArgs)));
					return null;
				}

				KeyScanCursor<byte[]> keyScanCursor = getConnection().scan(scanCursor, scanArgs);
				String nextCursorId = keyScanCursor.getCursor();

				List<byte[]> keys = keyScanCursor.getKeys();

				return new ScanIteration<byte[]>(Long.valueOf(nextCursorId), (keys));
			}
		}.open();

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return hScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, long cursorId, ScanOptions options) {

		return new KeyBoundCursor<Entry<byte[], byte[]>>(key, cursorId, options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline / transaction mode.");
				}

				com.lambdaworks.redis.ScanCursor scanCursor = getScanCursor(cursorId);
				ScanArgs scanArgs = getScanArgs(options);

				MapScanCursor<byte[], byte[]> mapScanCursor = getConnection().hscan(key, scanCursor, scanArgs);
				String nextCursorId = mapScanCursor.getCursor();

				Map<byte[], byte[]> values = mapScanCursor.getMap();
				return new ScanIteration<Entry<byte[], byte[]>>(Long.valueOf(nextCursorId), values.entrySet());
			}

		}.open();
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

				com.lambdaworks.redis.ScanCursor scanCursor = getScanCursor(cursorId);
				ScanArgs scanArgs = getScanArgs(options);

				ValueScanCursor<byte[]> valueScanCursor = getConnection().sscan(key, scanCursor, scanArgs);
				String nextCursorId = valueScanCursor.getCursor();

				List<byte[]> values = failsafeReadScanValues(valueScanCursor.getValues(), null);
				return new ScanIteration<byte[]>(Long.valueOf(nextCursorId), values);
			}
		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return zScan(key, 0L, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Tuple> zScan(byte[] key, long cursorId, ScanOptions options) {

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline / transaction mode.");
				}

				com.lambdaworks.redis.ScanCursor scanCursor = getScanCursor(cursorId);
				ScanArgs scanArgs = getScanArgs(options);

				ScoredValueScanCursor<byte[]> scoredValueScanCursor = getConnection().zscan(key, scanCursor, scanArgs);
				String nextCursorId = scoredValueScanCursor.getCursor();

				List<ScoredValue<byte[]>> result = scoredValueScanCursor.getValues();

				List<Tuple> values = failsafeReadScanValues(result, LettuceConverters.scoredValuesToTupleList());
				return new ScanIteration<Tuple>(Long.valueOf(nextCursorId), values);
			}
		}.open();
	}

	@SuppressWarnings("unchecked")
	private <T> T failsafeReadScanValues(List<?> source, @SuppressWarnings("rawtypes") Converter converter) {

		try {
			return (T) (converter != null ? converter.convert(source) : source);
		} catch (IndexOutOfBoundsException e) {
			// ignore this one
		}
		return null;
	}

	/**
	 * Specifies if pipelined and transaction results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Lettuce driver
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
		if (isQueueing()) {
			transaction(result);
		} else {
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
		if (asyncDedicatedConn == null) {
			if (this.pool != null) {
				this.asyncDedicatedConn = pool.getResource();
			} else {

				this.asyncDedicatedConn = client.connectAsync(CODEC);
				this.asyncDedicatedConn.select(dbIndex);
			}
		}
		return asyncDedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> getDedicatedConnection() {
		if (dedicatedConn == null) {
			this.dedicatedConn = syncConnection(getAsyncDedicatedConnection());
		}
		return dedicatedConn;
	}

	private com.lambdaworks.redis.RedisConnection<byte[], byte[]> syncConnection(
			RedisAsyncConnection<byte[], byte[]> asyncConnection) {
		try {
			return (com.lambdaworks.redis.RedisConnection<byte[], byte[]>) SYNC_HANDLER.invoke(null, asyncConnection,
					new Class[] { com.lambdaworks.redis.RedisConnection.class, RedisClusterConnection.class });
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

	private byte[][] extractScriptKeys(int numKeys, byte[]... keysAndArgs) {
		if (numKeys > 0) {
			return Arrays.copyOfRange(keysAndArgs, 0, numKeys);
		}
		return new byte[0][0];
	}

	private byte[][] extractScriptArgs(int numKeys, byte[]... keysAndArgs) {
		if (keysAndArgs.length > numKeys) {
			return Arrays.copyOfRange(keysAndArgs, numKeys, keysAndArgs.length);
		}
		return new byte[0][0];
	}

	private com.lambdaworks.redis.ScanCursor getScanCursor(long cursorId) {
		return com.lambdaworks.redis.ScanCursor.of(Long.toString(cursorId));
	}

	private ScanArgs getScanArgs(ScanOptions options) {
		if (options == null) {
			return null;
		}

		ScanArgs scanArgs = ScanArgs.Builder.matches(options.getPattern());
		if (options.getCount() != null) {
			scanArgs.limit(options.getCount());
		}

		return scanArgs;
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

	private void validateCommandIfRunningInTransactionMode(CommandType cmd, byte[]... args) {

		if (this.isQueueing()) {
			validateCommand(cmd, args);
		}
	}

	private void validateCommand(CommandType cmd, byte[]... args) {

		RedisCommand redisCommand = RedisCommand.failsafeCommandLookup(cmd.name());
		if (!RedisCommand.UNKNOWN.equals(redisCommand) && redisCommand.requiresArguments()) {
			try {
				redisCommand.validateArgumentCount(args != null ? args.length : 0);
			} catch (IllegalArgumentException e) {
				throw new InvalidDataAccessApiUsageException(String.format("Validation failed for %s command.", cmd), e);
			}
		}
	}

	@Override
	protected boolean isActive(RedisNode node) {

		if (node == null) {
			return false;
		}

		RedisConnection<String, String> connection = null;
		try {
			connection = client.connect(getRedisURI(node));
			return connection.ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	private RedisURI getRedisURI(RedisNode node) {
		return RedisURI.Builder.redis(node.getHost(), node.getPort()).build();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.AbstractRedisConnection#getSentinelConnection(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		RedisSentinelAsyncConnection<String, String> connection = client.connectSentinelAsync(getRedisURI(sentinel));
		return new LettuceSentinelConnection(connection);
	}

	/**
	 * {@link TypeHints} provide {@link CommandOutput} information for a given {@link CommandType}.
	 * 
	 * @since 1.2.1
	 */
	static class TypeHints {

		@SuppressWarnings("rawtypes")//
		private static final Map<CommandType, Class<? extends CommandOutput>> COMMAND_OUTPUT_TYPE_MAPPING = new HashMap<CommandType, Class<? extends CommandOutput>>();

		@SuppressWarnings("rawtypes")//
		private static final Map<Class<?>, Constructor<CommandOutput>> CONSTRUCTORS = new ConcurrentHashMap<Class<?>, Constructor<CommandOutput>>();

		{
			// INTEGER
			COMMAND_OUTPUT_TYPE_MAPPING.put(BITCOUNT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(BITOP, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DBSIZE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DECR, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DECRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DEL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(GETBIT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HDEL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HINCRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCR, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LINSERT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LPUSH, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LPUSHX, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LREM, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PTTL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PUBLISH, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RPUSH, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RPUSHX, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SADD, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SCARD, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SDIFFSTORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SETBIT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SETRANGE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SINTERSTORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SREM, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SUNIONSTORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(STRLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(TTL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZADD, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZCOUNT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZINTERSTORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZRANK, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREM, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREMRANGEBYRANK, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREMRANGEBYSCORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREVRANK, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZUNIONSTORE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PFCOUNT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PFMERGE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PFADD, IntegerOutput.class);

			// DOUBLE
			COMMAND_OUTPUT_TYPE_MAPPING.put(HINCRBYFLOAT, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCRBYFLOAT, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MGET, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZINCRBY, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZSCORE, DoubleOutput.class);

			// MAP
			COMMAND_OUTPUT_TYPE_MAPPING.put(HGETALL, MapOutput.class);

			// KEY LIST
			COMMAND_OUTPUT_TYPE_MAPPING.put(HKEYS, KeyListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(KEYS, KeyListOutput.class);

			// KEY VALUE
			COMMAND_OUTPUT_TYPE_MAPPING.put(BRPOP, KeyValueOutput.class);

			// SINGLE VALUE
			COMMAND_OUTPUT_TYPE_MAPPING.put(BRPOPLPUSH, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ECHO, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(GET, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(GETRANGE, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(GETSET, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HGET, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LINDEX, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LPOP, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RANDOMKEY, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RENAME, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RPOP, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RPOPLPUSH, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SPOP, ValueOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SRANDMEMBER, ValueOutput.class);

			// STATUS VALUE
			COMMAND_OUTPUT_TYPE_MAPPING.put(BGREWRITEAOF, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(BGSAVE, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(CLIENT, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DEBUG, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DISCARD, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(FLUSHALL, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(FLUSHDB, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HMSET, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INFO, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LSET, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LTRIM, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MIGRATE, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MSET, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(QUIT, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RESTORE, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SAVE, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SELECT, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SET, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SETEX, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SHUTDOWN, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SLAVEOF, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SYNC, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(TYPE, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(WATCH, StatusOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(UNWATCH, StatusOutput.class);

			// VALUE LIST
			COMMAND_OUTPUT_TYPE_MAPPING.put(HMGET, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MGET, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HVALS, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LRANGE, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SORT, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZRANGE, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZRANGEBYSCORE, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREVRANGE, ValueListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZREVRANGEBYSCORE, ValueListOutput.class);

			// BOOLEAN
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXISTS, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXPIRE, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXPIREAT, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HEXISTS, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSET, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSETNX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MOVE, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MSETNX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PERSIST, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PEXPIRE, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(PEXPIREAT, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(RENAMENX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SETNX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SISMEMBER, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SMOVE, BooleanOutput.class);

			// MULTI
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXEC, MultiOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MULTI, MultiOutput.class);

			// DATE
			COMMAND_OUTPUT_TYPE_MAPPING.put(LASTSAVE, DateOutput.class);

			// VALUE SET
			COMMAND_OUTPUT_TYPE_MAPPING.put(SDIFF, ValueSetOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SINTER, ValueSetOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SMEMBERS, ValueSetOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(SUNION, ValueSetOutput.class);
		}

		/**
		 * Returns the {@link CommandOutput} mapped for given {@link CommandType} or {@link ByteArrayOutput} as default.
		 * 
		 * @param type
		 * @return {@link ByteArrayOutput} as default when no matching {@link CommandOutput} available.
		 */
		@SuppressWarnings("rawtypes")
		public CommandOutput getTypeHint(CommandType type) {
			return getTypeHint(type, new ByteArrayOutput<byte[], byte[]>(CODEC));
		}

		/**
		 * Returns the {@link CommandOutput} mapped for given {@link CommandType} given {@link CommandOutput} as default.
		 * 
		 * @param type
		 * @return
		 */
		@SuppressWarnings("rawtypes")
		public CommandOutput getTypeHint(CommandType type, CommandOutput defaultType) {

			if (type == null || !COMMAND_OUTPUT_TYPE_MAPPING.containsKey(type)) {
				return defaultType;
			}
			CommandOutput<?, ?, ?> outputType = instanciateCommandOutput(COMMAND_OUTPUT_TYPE_MAPPING.get(type));
			return outputType != null ? outputType : defaultType;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private CommandOutput<?, ?, ?> instanciateCommandOutput(Class<? extends CommandOutput> type) {

			Assert.notNull(type, "Cannot create instance for 'null' type.");
			Constructor<CommandOutput> constructor = CONSTRUCTORS.get(type);
			if (constructor == null) {
				constructor = (Constructor<CommandOutput>) ClassUtils.getConstructorIfAvailable(type, RedisCodec.class);
				CONSTRUCTORS.put(type, constructor);
			}
			return BeanUtils.instantiateClass(constructor, CODEC);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

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

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfAdd(byte[], byte[][])
	 */
	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value.");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'.");

		try {
			if (isPipelined()) {
				if (values.length == 1) {
					pipeline(new LettuceResult(getAsyncConnection().pfadd(key, values[0])));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().pfadd(key, values[0], LettuceConverters.subarray(values, 1))));
				}
				return null;
			}
			if (isQueueing()) {
				if (values.length == 1) {
					transaction(new LettuceTxResult(getConnection().pfadd(key, values[0])));
				} else {
					transaction(new LettuceTxResult(getConnection().pfadd(key, values[0], LettuceConverters.subarray(values, 1))));
				}

				return null;
			}

			if (values.length == 1) {
				return getConnection().pfadd(key, values[0]);
			}

			return getConnection().pfadd(key, values[0], LettuceConverters.subarray(values, 1));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key.");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'.");
		try {
			if (isPipelined()) {
				if (keys.length == 1) {
					pipeline(new LettuceResult(getAsyncConnection().pfcount(keys[0])));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().pfcount(keys[0], LettuceConverters.subarray(keys, 1))));
				}
				return null;
			}
			if (isQueueing()) {
				if (keys.length == 1) {
					transaction(new LettuceTxResult(getConnection().pfcount(keys[0])));
				} else {
					transaction(new LettuceTxResult(getConnection().pfcount(keys[0], LettuceConverters.subarray(keys, 1))));
				}

				return null;
			}

			if (keys.length == 1) {
				return getConnection().pfcount(keys[0]);
			}

			return getConnection().pfcount(keys[0], LettuceConverters.subarray(keys, 1));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		Assert.notEmpty(sourceKeys, "PFMERGE requires at least one non 'null' source key.");
		Assert.noNullElements(sourceKeys, "source key for PFMERGE must not contain 'null'.");

		try {
			if (isPipelined()) {
				if (sourceKeys.length == 1) {
					pipeline(new LettuceResult(getAsyncConnection().pfmerge(destinationKey, sourceKeys[0])));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().pfmerge(destinationKey, sourceKeys[0],
							LettuceConverters.subarray(sourceKeys, 1))));
				}
			}
			if (isQueueing()) {
				if (sourceKeys.length == 1) {
					transaction(new LettuceTxResult(getConnection().pfmerge(destinationKey, sourceKeys[0])));
				} else {
					transaction(new LettuceTxResult(getConnection().pfmerge(destinationKey, sourceKeys[0],
							LettuceConverters.subarray(sourceKeys, 1))));
				}
			}

			if (sourceKeys.length == 1) {
				getConnection().pfmerge(destinationKey, sourceKeys[0]);
			} else {
				getConnection().pfmerge(destinationKey, sourceKeys[0], LettuceConverters.subarray(sourceKeys, 1));
			}
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[])
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key) {
		return zRangeByLex(key, Range.unbounded());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range) {
		return zRangeByLex(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYLEX.");

		String min = LettuceConverters.boundaryToBytesForZRangeByLex(range.getMin(), LettuceConverters.MINUS_BYTES);
		String max = LettuceConverters.boundaryToBytesForZRangeByLex(range.getMax(), LettuceConverters.PLUS_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(new LettuceResult(getAsyncConnection().zrangebylex(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(new LettuceResult(getAsyncConnection().zrangebylex(key, min, max),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit != null) {
					transaction(new LettuceTxResult(getConnection().zrangebylex(key, min, max, limit.getOffset(),
							limit.getCount()), LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(new LettuceTxResult(getConnection().zrangebylex(key, min, max),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}

			if (limit != null) {
				return LettuceConverters.bytesListToBytesSet().convert(
						getConnection().zrangebylex(key, min, max, limit.getOffset(), limit.getCount()));
			}

			return LettuceConverters.bytesListToBytesSet().convert(getConnection().zrangebylex(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

}
