/*
 * Copyright 2011-2017 the original author or authors.
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

import static io.lettuce.core.protocol.CommandType.*;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.beans.BeanUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceResultBuilder;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceStatusResult;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceTxResult;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceTxStatusResult;
import org.springframework.data.redis.core.RedisCommand;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

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
 * @author Ninad Divadkar
 */
public class LettuceConnection extends AbstractRedisConnection {

	static final RedisCodec<byte[], byte[]> CODEC = ByteArrayCodec.INSTANCE;

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			LettuceConverters.exceptionConverter());
	private static final TypeHints typeHints = new TypeHints();

	private final int defaultDbIndex;
	private int dbIndex;

	private final LettuceConnectionProvider connectionProvider;
	private final @Nullable StatefulConnection<byte[], byte[]> asyncSharedConn;
	private @Nullable StatefulConnection<byte[], byte[]> asyncDedicatedConn;

	private final long timeout;

	// refers only to main connection as pubsub happens on a different one
	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean isPipelined = false;
	private @Nullable List<LettuceResult> ppline;
	private final Queue<FutureResult<?>> txResults = new LinkedList<>();
	private volatile @Nullable LettuceSubscription subscription;
	/** flag indicating whether the connection needs to be dropped or not */
	private boolean convertPipelineAndTxResults = true;

	LettuceResult newLettuceResult(Future<?> resultHolder) {
		return newLettuceResult(resultHolder, (val) -> val);
	}

	<T> LettuceResult newLettuceResult(Future<T> resultHolder, Converter<T, ?> converter) {

		return LettuceResultBuilder.forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).build();
	}

	<T> LettuceResult newLettuceResult(Future<T> resultHolder, Converter<T, ?> converter, Supplier<?> defaultValue) {

		return LettuceResultBuilder.forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).defaultNullTo(defaultValue).build();
	}

	LettuceStatusResult newLettuceStatusResult(Future<?> resultHolder) {
		return new LettuceStatusResult(resultHolder);
	}

	LettuceTxResult newLettuceTxResult(Object resultHolder) {
		return newLettuceTxResult(resultHolder, (val) -> val);
	}

	<T> LettuceTxResult<T> newLettuceTxResult(Object resultHolder, Converter converter) {

		return LettuceResultBuilder.forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).buildTxResult();
	}

	<T> LettuceTxResult<T> newLettuceTxResult(Object resultHolder, Converter converter, Supplier<?> defaultValue) {

		return LettuceResultBuilder.forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).defaultNullTo(defaultValue).buildTxResult();
	}

	LettuceTxStatusResult newLettuceTxStatusResult(Object resultHolder) {
		return new LettuceTxStatusResult(resultHolder);
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
	 * @deprecated since 2.0, use pooling via {@link LettucePoolingClientConfiguration}.
	 */
	@Deprecated
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
	public LettuceConnection(@Nullable StatefulRedisConnection<byte[], byte[]> sharedConnection, long timeout,
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
	 * @deprecated since 2.0, use
	 *             {@link #LettuceConnection(StatefulRedisConnection, LettuceConnectionProvider, long, int)}
	 */
	@Deprecated
	public LettuceConnection(@Nullable StatefulRedisConnection<byte[], byte[]> sharedConnection, long timeout,
			RedisClient client, @Nullable LettucePool pool) {

		this(sharedConnection, timeout, client, pool, 0);
	}

	/**
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations.
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub connections.
	 * @param pool The connection pool to use for blocking and tx operations.
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 1.7
	 * @deprecated since 2.0, use
	 *             {@link #LettuceConnection(StatefulRedisConnection, LettuceConnectionProvider, long, int)}
	 */
	@Deprecated
	public LettuceConnection(@Nullable StatefulRedisConnection<byte[], byte[]> sharedConnection, long timeout,
			@Nullable AbstractRedisClient client, @Nullable LettucePool pool, int defaultDbIndex) {

		if (pool != null) {
			this.connectionProvider = new LettucePoolConnectionProvider(pool);
		} else {
			this.connectionProvider = new StandaloneConnectionProvider((RedisClient) client, CODEC);
		}

		this.asyncSharedConn = sharedConnection;
		this.timeout = timeout;
		this.defaultDbIndex = defaultDbIndex;
		this.dbIndex = this.defaultDbIndex;
	}

	/**
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations.
	 * @param connectionProvider connection provider to obtain and release native connections.
	 * @param timeout The connection timeout (in milliseconds)
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 2.0
	 */
	public LettuceConnection(StatefulRedisConnection<byte[], byte[]> sharedConnection,
			LettuceConnectionProvider connectionProvider, long timeout, int defaultDbIndex) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null.");

		this.asyncSharedConn = sharedConnection;
		this.connectionProvider = connectionProvider;
		this.timeout = timeout;
		this.defaultDbIndex = defaultDbIndex;
		this.dbIndex = this.defaultDbIndex;
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#geoCommands()
	 */
	@Override
	public RedisGeoCommands geoCommands() {
		return new LettuceGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hashCommands()
	 */
	@Override
	public RedisHashCommands hashCommands() {
		return new LettuceHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hyperLogLogCommands()
	 */
	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#keyCommands()
	 */
	@Override
	public RedisKeyCommands keyCommands() {
		return new LettuceKeyCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#listCommands()
	 */
	@Override
	public RedisListCommands listCommands() {
		return new LettuceListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#setCommands()
	 */
	@Override
	public RedisSetCommands setCommands() {
		return new LettuceSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#scriptingCommands()
	 */
	@Override
	public RedisScriptingCommands scriptingCommands() {
		return new LettuceScriptingCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#stringCommands()
	 */
	@Override
	public RedisStringCommands stringCommands() {
		return new LettuceStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#serverCommands()
	 */
	@Override
	public RedisServerCommands serverCommands() {
		return new LettuceServerCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#zSetCommands()
	 */
	@Override
	public RedisZSetCommands zSetCommands() {
		return new LettuceZSetCommands(this);
	}

	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object await(RedisFuture<?> cmd) {

		if (isMulti) {
			return null;
		}

		return LettuceFutures.awaitOrCancel(cmd, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public Object execute(String command, byte[]... args) {
		return execute(command, null, args);
	}

	/**
	 * 'Native' or 'raw' execution of the given command along-side the given arguments.
	 *
	 * @see RedisConnection#execute(String, byte[]...)
	 * @param command Command to execute
	 * @param commandOutputTypeHint Type of Output to use, may be (may be {@literal null}).
	 * @param args Possible command arguments (may be {@literal null})
	 * @return execution result.
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object execute(String command, @Nullable CommandOutput commandOutputTypeHint, byte[]... args) {

		Assert.hasText(command, "a valid command needs to be specified");
		try {
			String name = command.trim().toUpperCase();
			CommandType commandType = CommandType.valueOf(name);

			validateCommandIfRunningInTransactionMode(commandType, args);

			CommandArgs<byte[], byte[]> cmdArg = new CommandArgs<>(CODEC);
			if (!ObjectUtils.isEmpty(args)) {
				cmdArg.addKeys(args);
			}

			RedisClusterAsyncCommands<byte[], byte[]> connectionImpl = getAsyncConnection();

			CommandOutput expectedOutput = commandOutputTypeHint != null ? commandOutputTypeHint
					: typeHints.getTypeHint(commandType);
			Command cmd = new Command(commandType, expectedOutput, cmdArg);
			if (isPipelined()) {

				pipeline(newLettuceResult(connectionImpl.dispatch(cmd.getType(), cmd.getOutput(), cmd.getArgs())));
				return null;
			} else if (isQueueing()) {

				transaction(newLettuceTxResult(connectionImpl.dispatch(cmd.getType(), cmd.getOutput(), cmd.getArgs())));
				return null;
			} else {
				return await(connectionImpl.dispatch(cmd.getType(), cmd.getOutput(), cmd.getArgs()));
			}
		} catch (RedisException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void close() throws DataAccessException {

		super.close();

		if (isClosed) {
			return;
		}

		isClosed = true;

		if (asyncDedicatedConn != null) {
			try {
				connectionProvider.release(asyncDedicatedConn);
			} catch (RuntimeException ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		if (subscription != null) {
			if (subscription.isAlive()) {
				subscription.doClose();
			}
			subscription = null;
		}

		this.dbIndex = defaultDbIndex;
	}

	@Override
	public boolean isClosed() {
		return isClosed && !isSubscribed();
	}

	@Override
	public RedisClusterAsyncCommands<byte[], byte[]> getNativeConnection() {
		return (subscription != null ? subscription.pubsub.async() : getAsyncConnection());
	}

	@Override
	public boolean isQueueing() {
		return isMulti;
	}

	@Override
	public boolean isPipelined() {
		return isPipelined;
	}

	@Override
	public void openPipeline() {
		if (!isPipelined) {
			isPipelined = true;
			ppline = new ArrayList<>();
		}
	}

	@Override
	public List<Object> closePipeline() {

		if (isPipelined) {
			isPipelined = false;
			List<io.lettuce.core.protocol.RedisCommand<?, ?, ?>> futures = new ArrayList<>();
			for (LettuceResult<?, ?> result : ppline) {
				futures.add(result.getResultHolder());
			}

			try {
				boolean done = LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS,
						futures.toArray(new RedisFuture[futures.size()]));

				List<Object> results = new ArrayList<>(futures.size());

				Exception problem = null;

				if (done) {
					for (LettuceResult<?, ?> result : ppline) {

						if (result.getResultHolder().getOutput().hasError()) {

							Exception err = new InvalidDataAccessApiUsageException(result.getResultHolder().getOutput().getError());
							// remember only the first error
							if (problem == null) {
								problem = err;
							}
							results.add(err);
						} else if (!result.isStatus()) {

							try {
								results.add(result.seeksConversion() ? result.convert(result.get()) : result.get());
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
			} catch (Exception e) {
				throw new RedisPipelineException(e);
			}
		}

		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override

	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(newLettuceResult(getAsyncConnection().echo(message)));
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

	@Override
	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(newLettuceResult(getAsyncConnection().ping()));
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

	@Override
	public void discard() {
		isMulti = false;
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(((RedisAsyncCommands) getAsyncDedicatedConnection()).discard()));
				return;
			}
			((RedisCommands) getDedicatedConnection()).discard();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		} finally {
			txResults.clear();
		}
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<Object> exec() {

		isMulti = false;

		try {
			if (isPipelined()) {
				RedisFuture<TransactionResult> exec = ((RedisAsyncCommands) getAsyncDedicatedConnection()).exec();

				LettuceTransactionResultConverter resultConverter = new LettuceTransactionResultConverter(
						new LinkedList<>(txResults), LettuceConverters.exceptionConverter());

				pipeline(newLettuceResult(exec, source -> resultConverter
						.convert(LettuceConverters.transactionResultUnwrapper().convert((TransactionResult) source))));
				return null;
			}

			TransactionResult transactionResult = ((RedisCommands) getDedicatedConnection()).exec();
			List<Object> results = LettuceConverters.transactionResultUnwrapper().convert(transactionResult);
			return convertPipelineAndTxResults
					? new LettuceTransactionResultConverter(txResults, LettuceConverters.exceptionConverter()).convert(results)
					: results;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		} finally {
			txResults.clear();
		}
	}

	@Override
	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		try {
			if (isPipelined()) {
				((RedisAsyncCommands) getAsyncDedicatedConnection()).multi();
				return;
			}
			((RedisCommands) getDedicatedConnection()).multi();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
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
				transaction(new LettuceTxStatusResult(((RedisCommands) getAsyncConnection()).select(dbIndex)));
				return;
			}
			((RedisCommands) getConnection()).select(dbIndex);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(((RedisAsyncCommands) getAsyncDedicatedConnection()).unwatch()));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(((RedisAsyncCommands) getDedicatedConnection()).unwatch()));
				return;
			}
			((RedisCommands) getDedicatedConnection()).unwatch();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			if (isPipelined()) {
				pipeline(new LettuceStatusResult(((RedisAsyncCommands) getAsyncDedicatedConnection()).watch((Object[]) keys)));
				return;
			}
			if (isQueueing()) {
				transaction(new LettuceTxStatusResult(((RedisAsyncCommands) getDedicatedConnection()).watch((Object[]) keys)));
				return;
			}
			((RedisCommands) getDedicatedConnection()).watch((Object[]) keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(newLettuceResult(getAsyncConnection().publish(channel, message)));
				return null;
			}
			if (isQueueing()) {
				transaction(newLettuceTxResult(getConnection().publish(channel, message)));
				return null;
			}
			return getConnection().publish(channel, message);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	@Override
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

	@Override
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

	@SuppressWarnings("unchecked")
	<T> T failsafeReadScanValues(List<?> source, @SuppressWarnings("rawtypes") Converter converter) {

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

	/**
	 * {@link #close()} the current connection and open a new pub/sub connection to the Redis server.
	 *
	 * @return never {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected StatefulRedisPubSubConnection<byte[], byte[]> switchToPubSub() {

		close();
		return connectionProvider.getConnection(StatefulRedisPubSubConnection.class);
	}

	void pipeline(LettuceResult result) {
		if (isQueueing()) {
			transaction(result);
		} else {
			ppline.add(result);
		}
	}

	void transaction(FutureResult<?> result) {
		txResults.add(result);
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		if (isQueueing()) {
			return getAsyncDedicatedConnection();
		}
		if (asyncSharedConn != null) {

			if (asyncSharedConn instanceof StatefulRedisConnection) {
				return ((StatefulRedisConnection<byte[], byte[]>) asyncSharedConn).async();
			}
		}
		return getAsyncDedicatedConnection();
	}

	protected RedisClusterCommands<byte[], byte[]> getConnection() {

		if (isQueueing()) {
			return getDedicatedConnection();
		}
		if (asyncSharedConn != null) {

			if (asyncSharedConn instanceof StatefulRedisConnection) {
				return ((StatefulRedisConnection<byte[], byte[]>) asyncSharedConn).sync();
			}
			if (asyncSharedConn instanceof StatefulRedisClusterConnection) {
				return ((StatefulRedisClusterConnection<byte[], byte[]>) asyncSharedConn).sync();
			}
		}
		return getDedicatedConnection();
	}

	protected RedisClusterAsyncCommands<byte[], byte[]> getAsyncDedicatedConnection() {

		if (asyncDedicatedConn == null) {

			asyncDedicatedConn = doGetAsyncDedicatedConnection();

			if (asyncDedicatedConn instanceof StatefulRedisConnection) {
				((StatefulRedisConnection<byte[], byte[]>) asyncDedicatedConn).sync().select(dbIndex);
			}
		}

		if (asyncDedicatedConn instanceof StatefulRedisConnection) {
			return ((StatefulRedisConnection<byte[], byte[]>) asyncDedicatedConn).async();
		}
		if (asyncDedicatedConn instanceof StatefulRedisClusterConnection) {
			return ((StatefulRedisClusterConnection<byte[], byte[]>) asyncDedicatedConn).async();
		}

		throw new IllegalStateException(
				String.format("%s is not a supported connection type.", asyncDedicatedConn.getClass().getName()));
	}

	RedisClusterCommands<byte[], byte[]> getDedicatedConnection() {

		if (asyncDedicatedConn == null) {

			asyncDedicatedConn = doGetAsyncDedicatedConnection();

			if (asyncDedicatedConn instanceof StatefulRedisConnection) {
				((StatefulRedisConnection<byte[], byte[]>) asyncDedicatedConn).sync().select(dbIndex);
			}
		}

		if (asyncDedicatedConn instanceof StatefulRedisConnection) {
			return ((StatefulRedisConnection<byte[], byte[]>) asyncDedicatedConn).sync();
		}
		if (asyncDedicatedConn instanceof StatefulRedisClusterConnection) {
			return ((StatefulRedisClusterConnection<byte[], byte[]>) asyncDedicatedConn).sync();
		}

		throw new IllegalStateException(
				String.format("%s is not a supported connection type.", asyncDedicatedConn.getClass().getName()));
	}

	protected StatefulConnection<byte[], byte[]> doGetAsyncDedicatedConnection() {
		return connectionProvider.getConnection(StatefulConnection.class);
	}

	io.lettuce.core.ScanCursor getScanCursor(long cursorId) {
		return io.lettuce.core.ScanCursor.of(Long.toString(cursorId));
	}

	@Nullable
	ScanArgs getScanArgs(@Nullable ScanOptions options) {

		if (options == null) {
			return null;
		}

		ScanArgs scanArgs = new ScanArgs();

		if (options.getPattern() != null) {
			scanArgs.match(options.getPattern());
		}

		if (options.getCount() != null) {
			scanArgs.limit(options.getCount());
		}

		return scanArgs;
	}

	private void validateCommandIfRunningInTransactionMode(CommandType cmd, byte[]... args) {

		if (this.isQueueing()) {
			validateCommand(cmd, args);
		}
	}

	private void validateCommand(CommandType cmd, @Nullable byte[]... args) {

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

		StatefulRedisSentinelConnection<String, String> connection = null;
		try {
			connection = getConnection(node);
			return connection.sync().ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (connection != null) {
				connectionProvider.release(connection);
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

		StatefulRedisSentinelConnection<String, String> connection = getConnection(sentinel);
		return new LettuceSentinelConnection(connection);
	}

	@SuppressWarnings("unchecked")
	private StatefulRedisSentinelConnection<String, String> getConnection(RedisNode sentinel) {
		return ((TargetAware) connectionProvider).getConnection(StatefulRedisSentinelConnection.class,
				getRedisURI(sentinel));
	}

	LettuceConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	/**
	 * {@link TypeHints} provide {@link CommandOutput} information for a given {@link CommandType}.
	 *
	 * @since 1.2.1
	 */
	static class TypeHints {

		@SuppressWarnings("rawtypes") //
		private static final Map<CommandType, Class<? extends CommandOutput>> COMMAND_OUTPUT_TYPE_MAPPING = new HashMap<>();

		@SuppressWarnings("rawtypes") //
		private static final Map<Class<?>, Constructor<CommandOutput>> CONSTRUCTORS = new ConcurrentHashMap<>();

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
			return getTypeHint(type, new ByteArrayOutput<>(CODEC));
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

	@RequiredArgsConstructor
	private class LettucePoolConnectionProvider implements LettuceConnectionProvider {

		private final LettucePool pool;

		@Override
		public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {
			return connectionType.cast(pool.getResource());
		}

		@Override
		@SuppressWarnings("unchecked")
		public void release(StatefulConnection<?, ?> connection) {

			if (connection.isOpen()) {
				pool.returnResource((StatefulConnection<byte[], byte[]>) connection);
			} else {
				pool.returnBrokenResource((StatefulConnection<byte[], byte[]>) connection);
			}
		}
	}
}
