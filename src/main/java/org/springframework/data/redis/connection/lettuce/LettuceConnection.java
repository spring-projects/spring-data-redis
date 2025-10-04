/*
 * Copyright 2011-2025 the original author or authors.
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

import static io.lettuce.core.protocol.CommandType.*;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
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
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeanUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceResultBuilder;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.RedisCommand;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/mp911de/lettuce">Lettuce</a> Redis
 * client.
 * <p>
 * While the underlying Lettuce {@literal RedisClient} and {@literal StatefulRedisConnection} instances used by
 * {@link LettuceConnection} are Thread-safe, this class itself is not Thread-safe. Therefore, instances of
 * {@link LettuceConnection} should not be shared across multiple Threads when executing Redis commands and other
 * operations. If optimal performance is required by your application(s), then we recommend direct access to the
 * low-level, API provided by the underlying Lettuce client library (driver), where such Thread-safety guarantees can be
 * made. Simply call {@link #getNativeConnection()} and use the native resource as required.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Tamil Selvan
 * @author ihaohong
 * @author John Blum
 */
@NullUnmarked
public class LettuceConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			LettuceExceptionConverter.INSTANCE);

	static final RedisCodec<byte[], byte[]> CODEC = ByteArrayCodec.INSTANCE;

	private static final TypeHints typeHints = new TypeHints();

	private static class LettuceTransactionResultConverter<T> extends TransactionResultConverter<T> {

		public LettuceTransactionResultConverter(Queue<FutureResult<T>> txResults,
				Converter<Exception, DataAccessException> exceptionConverter) {

			super(txResults, exceptionConverter);
		}

		@Override
		public @Nullable List<@Nullable Object> convert(List<Object> execResults) {
			// Lettuce Empty list means null (watched variable modified)
			return execResults.isEmpty() ? null : super.convert(execResults);
		}
	}

	// refers only to main connection as pubsub happens on a different one
	private boolean convertPipelineAndTxResults = true;
	private boolean isClosed = false;
	private boolean isMulti = false;
	private boolean isPipelined = false;

	private int dbIndex;
	private final int defaultDbIndex;
	private final long timeout;

	private final LettuceConnectionProvider connectionProvider;

	private volatile @Nullable LettuceSubscription subscription;

	private final LettuceGeoCommands geoCommands = new LettuceGeoCommands(this);
	private final LettuceHashCommands hashCommands = new LettuceHashCommands(this);
	private final LettuceHyperLogLogCommands hyperLogLogCommands = new LettuceHyperLogLogCommands(this);
	private final LettuceKeyCommands keyCommands = new LettuceKeyCommands(this);
	private final LettuceListCommands listCommands = new LettuceListCommands(this);
	private final LettuceScriptingCommands scriptingCommands = new LettuceScriptingCommands(this);
	private final LettuceSetCommands setCommands = new LettuceSetCommands(this);
	private final LettuceServerCommands serverCommands = new LettuceServerCommands(this);
	private final LettuceStreamCommands streamCommands = new LettuceStreamCommands(this);
	private final LettuceStringCommands stringCommands = new LettuceStringCommands(this);
	private final LettuceZSetCommands zSetCommands = new LettuceZSetCommands(this);

	private @Nullable List<LettuceResult<?, ?>> ppline;

	private final Log LOGGER = LogFactory.getLog(getClass());

	private PipeliningFlushPolicy pipeliningFlushPolicy = PipeliningFlushPolicy.flushEachCommand();

	private @Nullable PipeliningFlushState pipeliningFlushState;

	private final Queue<FutureResult<?>> txResults = new LinkedList<>();

	private @Nullable StatefulConnection<byte[], byte[]> asyncDedicatedConnection;
	private final @Nullable StatefulConnection<byte[], byte[]> asyncSharedConnection;

	/**
	 * Creates a new {@link LettuceConnection}.
	 *
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when instantiating a native connection
	 */
	public LettuceConnection(long timeout,  @NonNull RedisClient client) {
		this(null, timeout, client);
	}

	/**
	 * Creates a new {@link LettuceConnection}.
	 *
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Will not be used
	 *          for transactions or blocking operations
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub, blocking, and tx connections
	 */
	public LettuceConnection(@Nullable StatefulRedisConnection<byte[], byte[]> sharedConnection, long timeout,
		@NonNull RedisClient client) {
		this(sharedConnection, timeout, client, 0);
	}

	/**
	 * Creates a new {@link LettuceConnection}.
	 *
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations.
	 * @param timeout The connection timeout (in milliseconds)
	 * @param client The {@link RedisClient} to use when making pub/sub connections.
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 1.7
	 */
	public LettuceConnection(@Nullable  StatefulRedisConnection<byte[], byte[]> sharedConnection, long timeout,
			@NonNull  AbstractRedisClient client, int defaultDbIndex) {

		this.connectionProvider = new StandaloneConnectionProvider((RedisClient) client, CODEC);
		this.asyncSharedConnection = sharedConnection;
		this.timeout = timeout;
		this.defaultDbIndex = defaultDbIndex;
		this.dbIndex = this.defaultDbIndex;
	}

	/**
	 * Creates a new {@link LettuceConnection}.
	 *
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations.
	 * @param connectionProvider connection provider to obtain and release native connections.
	 * @param timeout The connection timeout (in milliseconds)
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 2.0
	 */
	public LettuceConnection(@Nullable StatefulRedisConnection<byte[], byte[]> sharedConnection,
		@NonNull LettuceConnectionProvider connectionProvider, long timeout, int defaultDbIndex) {

		this((StatefulConnection<byte[], byte[]>) sharedConnection, connectionProvider, timeout, defaultDbIndex);
	}

	/**
	 * Creates a new {@link LettuceConnection}.
	 *
	 * @param sharedConnection A native connection that is shared with other {@link LettuceConnection}s. Should not be
	 *          used for transactions or blocking operations.
	 * @param connectionProvider connection provider to obtain and release native connections.
	 * @param timeout The connection timeout (in milliseconds)
	 * @param defaultDbIndex The db index to use along with {@link RedisClient} when establishing a dedicated connection.
	 * @since 2.1
	 */
	LettuceConnection(@Nullable StatefulConnection<byte[], byte[]> sharedConnection,
		@NonNull LettuceConnectionProvider connectionProvider, long timeout, int defaultDbIndex) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null");

		this.asyncSharedConnection = sharedConnection;
		this.connectionProvider = connectionProvider;
		this.timeout = timeout;
		this.defaultDbIndex = defaultDbIndex;
		this.dbIndex = this.defaultDbIndex;
	}

	@Override
	public org.springframework.data.redis.connection.RedisCommands commands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return this.geoCommands;
	}

	@Override
	public RedisHashCommands hashCommands() {
		return this.hashCommands;
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return this.hyperLogLogCommands;
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return this.keyCommands;
	}

	@Override
	public RedisListCommands listCommands() {
		return this.listCommands;
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return this.scriptingCommands;
	}

	@Override
	public RedisSetCommands setCommands() {
		return this.setCommands;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return this.serverCommands;
	}

	@Override
	public RedisStreamCommands streamCommands() {
		return this.streamCommands;
	}

	@Override
	public RedisStringCommands stringCommands() {
		return this.stringCommands;
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return this.zSetCommands;
	}

	protected DataAccessException convertLettuceAccessException(Exception cause) {
		return EXCEPTION_TRANSLATION.translate(cause);
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

		Assert.hasText(command, () -> "A valid command [%s] needs to be specified".formatted(command));

		ProtocolKeyword commandType = getCommandType(command.trim().toUpperCase());

		validateCommandIfRunningInTransactionMode(commandType, args);

		CommandArgs<byte[], byte[]> commandArguments = new CommandArgs<>(CODEC);

		if (!ObjectUtils.isEmpty(args)) {
			commandArguments.addKeys(args);
		}

		CommandOutput expectedOutput = commandOutputTypeHint != null ? commandOutputTypeHint
				: typeHints.getTypeHint(commandType);

		Command redisCommand = new Command(commandType, expectedOutput, commandArguments);

		return invoke().just(RedisClusterAsyncCommands::dispatch, redisCommand.getType(), redisCommand.getOutput(),
				redisCommand.getArgs());
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {

		if (isQueueing() || isPipelined()) {
			return getAsyncDedicatedConnection();
		}

		StatefulConnection<byte[], byte[]> sharedConnection = this.asyncSharedConnection;

		if (sharedConnection != null) {
			if (sharedConnection instanceof StatefulRedisConnection<byte[], byte[]> statefulConnection) {
				return statefulConnection.async();
			}
			if (sharedConnection instanceof StatefulRedisClusterConnection<byte[], byte[]> statefulClusterConnection) {
				return statefulClusterConnection.async();
			}
		}

		return getAsyncDedicatedConnection();
	}

	/**
	 * Obtain a {@link LettuceInvoker} to call Lettuce methods using the default {@link #getAsyncConnection() connection}.
	 *
	 * @return the {@link LettuceInvoker}.
	 * @since 2.5
	 */
	LettuceInvoker invoke() {
		return invoke(getAsyncConnection());
	}

	/**
	 * Obtain a {@link LettuceInvoker} to call Lettuce methods using the given {@link RedisClusterAsyncCommands
	 * connection}.
	 *
	 * @param connection the connection to use.
	 * @return the {@link LettuceInvoker}.
	 * @since 2.5
	 */
	LettuceInvoker invoke(RedisClusterAsyncCommands<byte[], byte[]> connection) {
		return doInvoke(connection, false);
	}

	/**
	 * Obtain a {@link LettuceInvoker} to call Lettuce methods returning a status response using the default
	 * {@link #getAsyncConnection() connection}. Status responses are not included in transactional and pipeline results.
	 *
	 * @return the {@link LettuceInvoker}.
	 * @since 2.5
	 */
	LettuceInvoker invokeStatus() {
		return doInvoke(getAsyncConnection(), true);
	}

	private LettuceInvoker doInvoke(RedisClusterAsyncCommands<byte[], byte[]> connection, boolean statusCommand) {

		if (isPipelined()) {

			return new LettuceInvoker(connection, (future, converter, nullDefault) -> {

				try {
					if (statusCommand) {
						pipeline(newLettuceStatusResult(future.get()));
					} else {
						pipeline(newLettuceResult(future.get(), converter, nullDefault));
					}
				} catch (Exception ex) {
					throw convertLettuceAccessException(ex);
				}

				return null;
			});
		}

		if (isQueueing()) {

			return new LettuceInvoker(connection, (future, converter, nullDefault) -> {

				try {
					if (statusCommand) {
						transaction(newLettuceStatusResult(future.get()));
					} else {
						transaction(newLettuceResult(future.get(), converter, nullDefault));
					}
				} catch (Exception ex) {
					throw convertLettuceAccessException(ex);
				}

				return null;
			});
		}

		return new LettuceInvoker(connection, (future, converter, nullDefault) -> {

			try {

				Object result = await(future.get());

				return result != null ? converter.convert(result) : nullDefault.get();
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}
		});
	}

	<T, R> LettuceResult<T, R> newLettuceResult(Future<T> resultHolder, Converter<T, R> converter) {

		return LettuceResultBuilder.<T, R> forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(this.convertPipelineAndTxResults).build();
	}

	<T, R> LettuceResult<T, R> newLettuceResult(Future<T> resultHolder, Converter<T, R> converter,
			Supplier<R> defaultValue) {

		return LettuceResultBuilder.<T, R> forResponse(resultHolder).mappedWith(converter)
				.convertPipelineAndTxResults(this.convertPipelineAndTxResults).defaultNullTo(defaultValue).build();
	}

	<T, R> LettuceResult<T, R> newLettuceStatusResult(Future<T> resultHolder) {
		return LettuceResultBuilder.<T, R> forResponse(resultHolder).buildStatusResult();
	}

	void pipeline(LettuceResult<?, ?> result) {

		PipeliningFlushState pipeliningFlushState = this.pipeliningFlushState;

		if (pipeliningFlushState != null) {
			pipeliningFlushState.onCommand(getOrCreateDedicatedConnection());
		}

		if (isQueueing()) {
			transaction(result);
		} else {
			this.ppline.add(result);
		}
	}

	void transaction(FutureResult<?> result) {
		this.txResults.add(result);
	}

	@Override
	public void close() {

		super.close();

		if (isClosed) {
			return;
		}

		isClosed = true;

		try {
			reset();
		} catch (RuntimeException ex) {
			LOGGER.debug("Failed to reset connection during close", ex);
		}
	}

	private void reset() {

		if (this.asyncDedicatedConnection != null) {
			try {
				if (customizedDatabaseIndex()) {
					potentiallySelectDatabase(this.asyncDedicatedConnection, this.defaultDbIndex);
				}
				this.connectionProvider.release(this.asyncDedicatedConnection);
				this.asyncDedicatedConnection = null;
			} catch (RuntimeException ex) {
				throw convertLettuceAccessException(ex);
			}
		}

		LettuceSubscription subscription = this.subscription;

		if (isAlive(subscription)) {
			subscription.doClose();
		}

		this.subscription = null;
		this.dbIndex = defaultDbIndex;
	}

	@Override
	public boolean isClosed() {
		return this.isClosed && !isSubscribed();
	}

	@Override
	public RedisClusterAsyncCommands<byte[], byte[]> getNativeConnection() {

		LettuceSubscription subscription = this.subscription;

		return isAlive(subscription) ? subscription.getNativeConnection().async() : getAsyncConnection();
	}

	@Contract("null -> false")
	private boolean isAlive(@Nullable LettuceSubscription subscription) {
		return subscription != null && subscription.isAlive();
	}

	@Override
	public boolean isQueueing() {
		return this.isMulti;
	}

	@Override
	public boolean isPipelined() {
		return this.isPipelined;
	}

	@Override
	public void openPipeline() {

		if (!isPipelined) {
			isPipelined = true;
			ppline = new ArrayList<>();
			pipeliningFlushState = this.pipeliningFlushPolicy.newPipeline();
			pipeliningFlushState.onOpen(this.getOrCreateDedicatedConnection());
		}
	}

	@Override
	public List<@Nullable Object> closePipeline() {

		if (!isPipelined) {
			return Collections.emptyList();
		}

		pipeliningFlushState.onClose(this.getOrCreateDedicatedConnection());
		pipeliningFlushState = null;
		isPipelined = false;

		List<CompletableFuture<?>> futures = new ArrayList<>(ppline.size());

		for (LettuceResult<?, ?> result : ppline) {
			futures.add(result.getResultHolder());
		}

		try {

			boolean done = LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));

			List<Object> results = new ArrayList<>(futures.size());

			Exception problem = null;

			if (done) {
				for (LettuceResult<?, ?> result : ppline) {

					CompletableFuture<?> resultHolder = result.getResultHolder();
					if (resultHolder.isCompletedExceptionally()) {

						String message;
						if (resultHolder instanceof io.lettuce.core.protocol.RedisCommand<?, ?, ?> rc) {
							message = rc.getOutput().getError();
						} else {
							try {
								resultHolder.get();
								message = "";
							} catch (InterruptedException ignore) {
								message = "";
							} catch (ExecutionException e) {
								message = e.getCause().getMessage();
							}
						}

						Exception exception = new InvalidDataAccessApiUsageException(message);

						// remember only the first error
						if (problem == null) {
							problem = exception;
						}

						results.add(exception);
					} else if (!result.isStatus()) {

						try {
							results.add(result.conversionRequired() ? result.convert(result.get()) : result.get());
						} catch (DataAccessException ex) {
							if (problem == null) {
								problem = ex;
							}
							results.add(ex);
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
		} catch (Exception ex) {
			throw new RedisPipelineException(ex);
		}
	}

	@Override
	public  byte@Nullable [] echo(byte[] message) {
		return invoke().just(RedisClusterAsyncCommands::echo, message);
	}

	@Override
	public @Nullable String ping() {
		return invoke().just(RedisClusterAsyncCommands::ping);
	}

	@Override
	public void discard() {

		isMulti = false;

		try {
			if (isPipelined()) {
				pipeline(newLettuceStatusResult(getAsyncDedicatedRedisCommands().discard()));
				return;
			}
			getDedicatedRedisCommands().discard();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		} finally {
			txResults.clear();
		}
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public @Nullable List<@Nullable Object> exec() {

		isMulti = false;

		try {
			Converter<Exception, DataAccessException> exceptionConverter = this::convertLettuceAccessException;

			if (isPipelined()) {
				RedisFuture<TransactionResult> exec = getAsyncDedicatedRedisCommands().exec();

				LettuceTransactionResultConverter resultConverter = new LettuceTransactionResultConverter(
						new LinkedList<>(txResults), exceptionConverter);

				pipeline(newLettuceResult(exec,
						source -> resultConverter.convert(LettuceConverters.transactionResultUnwrapper().convert(source))));

				return null;
			}

			TransactionResult transactionResult = getDedicatedRedisCommands().exec();

			List<Object> results = LettuceConverters.transactionResultUnwrapper().convert(transactionResult);

			return convertPipelineAndTxResults
					? new LettuceTransactionResultConverter(txResults, exceptionConverter).convert(results)
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
				getAsyncDedicatedRedisCommands().multi();
				return;
			}
			getDedicatedRedisCommands().multi();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {

		if (asyncSharedConnection != null) {
			throw new InvalidDataAccessApiUsageException("Selecting a new database not supported due to shared connection;"
					+ " Use separate ConnectionFactory instances to work with multiple databases");
		}

		this.dbIndex = dbIndex;

		invokeStatus().just(RedisClusterAsyncCommands::dispatch, CommandType.SELECT,
				new StatusOutput<>(ByteArrayCodec.INSTANCE), new CommandArgs<>(ByteArrayCodec.INSTANCE).add(dbIndex));
	}

	@Override
	public void unwatch() {

		try {
			if (isPipelined()) {
				pipeline(newLettuceStatusResult(getAsyncDedicatedRedisCommands().unwatch()));
				return;
			}
			if (isQueueing()) {
				transaction(newLettuceStatusResult(getAsyncDedicatedRedisCommands().unwatch()));
				return;
			}
			getDedicatedRedisCommands().unwatch();
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void watch(byte[]... keys) {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
		}

		try {
			if (isPipelined()) {
				pipeline(newLettuceStatusResult(getAsyncDedicatedRedisCommands().watch(keys)));
				return;
			}
			if (isQueueing()) {
				transaction(newLettuceStatusResult(getAsyncDedicatedRedisCommands().watch(keys)));
				return;
			}
			getDedicatedRedisCommands().watch(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte[] channel, byte[] message) {
		return invoke().just(RedisClusterAsyncCommands::publish, channel, message);
	}

	@Override
	public @Nullable Subscription getSubscription() {
		return this.subscription;
	}

	@Override
	public boolean isSubscribed() {
		Subscription subscription = getSubscription();
		return subscription != null && subscription.isAlive();
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {

		checkSubscription();

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Transaction/Pipelining is not supported for Pub/Sub subscriptions");
		}

		try {
			this.subscription = initSubscription(listener);
			this.subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {

		checkSubscription();

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Transaction/Pipelining is not supported for Pub/Sub subscriptions");
		}

		try {
			this.subscription = initSubscription(listener);
			this.subscription.subscribe(channels);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	<T> @Nullable T failsafeReadScanValues(List<?> source, @SuppressWarnings("rawtypes") @Nullable Converter converter) {

		try {
			return (T) (converter != null ? converter.convert(source) : source);
		} catch (IndexOutOfBoundsException ignore) {}

		return null;
	}

	/**
	 * Specifies if pipelined and transaction results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Lettuce driver
	 *
	 * @param convertPipelineAndTxResults whether to convert pipeline and tx results.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/**
	 * Configures the flushing policy when using pipelining.
	 *
	 * @param pipeliningFlushPolicy the flushing policy to control when commands get written to the Redis connection.
	 * @see PipeliningFlushPolicy#flushEachCommand()
	 * @see #openPipeline()
	 * @see StatefulRedisConnection#flushCommands()
	 * @since 2.3
	 */
	public void setPipeliningFlushPolicy(PipeliningFlushPolicy pipeliningFlushPolicy) {

		Assert.notNull(pipeliningFlushPolicy, "PipeliningFlushingPolicy must not be null");

		this.pipeliningFlushPolicy = pipeliningFlushPolicy;
	}

	/**
	 * {@link #close()} the current connection and open a new pub/sub connection to the Redis server.
	 *
	 * @return never {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected StatefulRedisPubSubConnection<byte[], byte[]> switchToPubSub() {

		checkSubscription();
		reset();

		return this.connectionProvider.getConnection(StatefulRedisPubSubConnection.class);
	}

	/**
	 * Customization hook to create a {@link LettuceSubscription}.
	 *
	 * @param listener the {@link MessageListener} to notify.
	 * @param connection Pub/Sub connection.
	 * @param connectionProvider the {@link LettuceConnectionProvider} for connection release.
	 * @return a {@link LettuceSubscription}.
	 * @since 2.2
	 */
	protected LettuceSubscription doCreateSubscription(MessageListener listener,
			StatefulRedisPubSubConnection<byte[], byte[]> connection, LettuceConnectionProvider connectionProvider) {

		return new LettuceSubscription(listener, connection, connectionProvider);
	}

	protected RedisClusterCommands<byte[], byte[]> getConnection() {

		if (isQueueing()) {
			return getDedicatedConnection();
		}

		if (asyncSharedConnection != null) {
			if (asyncSharedConnection instanceof StatefulRedisConnection<byte[], byte[]> statefulConnection) {
				return statefulConnection.sync();
			}
			if (asyncSharedConnection instanceof StatefulRedisClusterConnection<byte[], byte[]> statefulClusterConnection) {
				return statefulClusterConnection.sync();
			}
		}

		return getDedicatedConnection();
	}

	RedisClusterCommands<byte[], byte[]> getDedicatedConnection() {

		StatefulConnection<byte[], byte[]> connection = getOrCreateDedicatedConnection();

		if (connection instanceof StatefulRedisConnection<byte[], byte[]> statefulConnection) {
			return statefulConnection.sync();
		}
		if (connection instanceof StatefulRedisClusterConnection<byte[], byte[]> statefulClusterConnection) {
			return statefulClusterConnection.sync();
		}

		throw new IllegalStateException("%s is not a supported connection type".formatted(connection.getClass().getName()));
	}

	protected RedisClusterAsyncCommands<byte[], byte[]> getAsyncDedicatedConnection() {

		if (isClosed()) {
			throw new RedisSystemException("Connection is closed", null);
		}

		StatefulConnection<byte[], byte[]> connection = getOrCreateDedicatedConnection();

		if (connection instanceof StatefulRedisConnection<byte[], byte[]> statefulConnection) {
			return statefulConnection.async();
		}

		if (asyncDedicatedConnection instanceof StatefulRedisClusterConnection<byte[], byte[]> statefulClusterConnection) {
			return statefulClusterConnection.async();
		}

		throw new IllegalStateException("%s is not a supported connection type".formatted(connection.getClass().getName()));
	}

	@SuppressWarnings("unchecked")
	protected StatefulConnection<byte[], byte[]> doGetAsyncDedicatedConnection() {
		return getConnectionProvider().getConnection(StatefulConnection.class);
	}

	@Override
	protected boolean isActive(RedisNode node) {

		StatefulRedisSentinelConnection<String, String> connection = null;

		try {
			connection = getConnection(node);
			return connection.sync().ping().equalsIgnoreCase("pong");
		} catch (Exception ignore) {
			return false;
		} finally {
			if (connection != null) {
				getConnectionProvider().release(connection);
			}
		}
	}

	@Override
	protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {

		StatefulRedisSentinelConnection<String, String> connection = getConnection(sentinel);

		return new LettuceSentinelConnection(connection);
	}

	LettuceConnectionProvider getConnectionProvider() {
		return this.connectionProvider;
	}

	@SuppressWarnings("unchecked")
	private StatefulRedisSentinelConnection<String, String> getConnection(RedisNode sentinel) {
		return ((TargetAware) getConnectionProvider()).getConnection(StatefulRedisSentinelConnection.class,
				getRedisURI(sentinel));
	}

	private @Nullable <T> T await(RedisFuture<T> cmd) {

		if (this.isMulti) {
			return null;
		}

		try {
			return LettuceFutures.awaitOrCancel(cmd, timeout, TimeUnit.MILLISECONDS);
		} catch (RuntimeException ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private StatefulConnection<byte[], byte[]> getOrCreateDedicatedConnection() {

		if (this.asyncDedicatedConnection == null) {
			this.asyncDedicatedConnection = doGetAsyncDedicatedConnection();
		}

		return this.asyncDedicatedConnection;
	}

	private RedisCommands<byte[], byte[]> getDedicatedRedisCommands() {
		return (RedisCommands<byte[], byte[]>) getDedicatedConnection();
	}

	private RedisAsyncCommands<byte[], byte[]> getAsyncDedicatedRedisCommands() {
		return (RedisAsyncCommands<byte[], byte[]>) getAsyncDedicatedConnection();
	}

	private void checkSubscription() {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
	}

	private LettuceSubscription initSubscription(MessageListener listener) {
		return doCreateSubscription(listener, switchToPubSub(), connectionProvider);
	}

	private RedisURI getRedisURI(RedisNode node) {
		return RedisURI.Builder.redis(node.getRequiredHost(), getPort(node)).build();
	}

	private int getPort(RedisNode node) {
		Integer port = node.getPort();
		return port != null ? port : RedisURI.DEFAULT_REDIS_PORT;
	}

	private boolean customizedDatabaseIndex() {
		return defaultDbIndex != dbIndex;
	}

	private static void potentiallySelectDatabase(StatefulConnection<byte[], byte[]> connection, int dbIndex) {

		if (connection instanceof StatefulRedisConnection<byte[], byte[]> statefulConnection) {
			statefulConnection.sync().select(dbIndex);
		}
	}

	io.lettuce.core.ScanCursor getScanCursor(CursorId cursorId) {
		return io.lettuce.core.ScanCursor.of(cursorId.getCursorId());
	}

	private void validateCommandIfRunningInTransactionMode(ProtocolKeyword cmd, byte[]... args) {

		if (this.isQueueing()) {
			validateCommand(cmd, args);
		}
	}

	private void validateCommand(ProtocolKeyword command, byte @Nullable[]... args) {

		RedisCommand redisCommand = RedisCommand.failsafeCommandLookup(command.toString());

		if (!RedisCommand.UNKNOWN.equals(redisCommand) && redisCommand.requiresArguments()) {
			try {
				redisCommand.validateArgumentCount(args != null ? args.length : 0);
			} catch (IllegalArgumentException ex) {
				throw new InvalidDataAccessApiUsageException("Validation failed for %s command".formatted(command), ex);
			}
		}
	}

	private static ProtocolKeyword getCommandType(String name) {

		try {
			return CommandType.valueOf(name);
		} catch (IllegalArgumentException ignore) {
			return new CustomCommandType(name);
		}
	}

	/**
	 * {@link TypeHints} provide {@link CommandOutput} information for a given {@link CommandType}.
	 *
	 * @since 1.2.1
	 */
	static class TypeHints {

		@SuppressWarnings("rawtypes") //
		private static final Map<ProtocolKeyword, Class<? extends CommandOutput>> COMMAND_OUTPUT_TYPE_MAPPING = new HashMap<>();

		@SuppressWarnings("rawtypes") //
		private static final Map<Class<?>, Constructor<CommandOutput>> CONSTRUCTORS = new ConcurrentHashMap<>();

		static {
			// INTEGER
			COMMAND_OUTPUT_TYPE_MAPPING.put(BITCOUNT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(BITOP, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(BITPOS, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DBSIZE, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DECR, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DECRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(DEL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(GETBIT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HDEL, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HINCRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSTRLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCR, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCRBY, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LINSERT, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LPUSH, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(LPOS, IntegerOutput.class);
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
			COMMAND_OUTPUT_TYPE_MAPPING.put(XLEN, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(XTRIM, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZADD, IntegerOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZCARD, IntegerOutput.class);
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

			COMMAND_OUTPUT_TYPE_MAPPING.put(HEXPIRE, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HEXPIREAT, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HPEXPIRE, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HPEXPIREAT, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HPERSIST, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HTTL, IntegerListOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HPTTL, IntegerListOutput.class);

			// DOUBLE
			COMMAND_OUTPUT_TYPE_MAPPING.put(HINCRBYFLOAT, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(INCRBYFLOAT, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZINCRBY, DoubleOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZSCORE, DoubleOutput.class);

			// DOUBLE LIST
			COMMAND_OUTPUT_TYPE_MAPPING.put(ZMSCORE, DoubleListOutput.class);

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
            COMMAND_OUTPUT_TYPE_MAPPING.put(HGETDEL, ValueListOutput.class);
            COMMAND_OUTPUT_TYPE_MAPPING.put(HGETEX, ValueListOutput.class);

            // BOOLEAN
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXISTS, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXPIRE, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(EXPIREAT, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HEXISTS, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSET, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSETEX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(HSETNX, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(MOVE, BooleanOutput.class);
			COMMAND_OUTPUT_TYPE_MAPPING.put(COPY, BooleanOutput.class);
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
		 * @param type {@link ProtocolKeyword} used to lookup the type hint.
		 * @return {@link ByteArrayOutput} as default when no matching {@link CommandOutput} available.
		 */
		@SuppressWarnings("rawtypes")
		public CommandOutput getTypeHint(ProtocolKeyword type) {
			return getTypeHint(type, new ByteArrayOutput<>(CODEC));
		}

		/**
		 * Returns the {@link CommandOutput} mapped for given {@link CommandType} given {@link CommandOutput} as default.
		 *
		 * @param type {@link ProtocolKeyword} used to lookup the type hint.
		 * @return the {@link CommandOutput} mapped for given {@link CommandType} given {@link CommandOutput} as default.
		 */
		@SuppressWarnings("rawtypes")
		public CommandOutput getTypeHint(@Nullable ProtocolKeyword type, CommandOutput defaultType) {

			if (type == null || !COMMAND_OUTPUT_TYPE_MAPPING.containsKey(type)) {
				return defaultType;
			}

			CommandOutput<?, ?, ?> outputType = instantiateCommandOutput(COMMAND_OUTPUT_TYPE_MAPPING.get(type));

			return outputType != null ? outputType : defaultType;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private CommandOutput<?, ?, ?> instantiateCommandOutput(Class<? extends CommandOutput> type) {

			Assert.notNull(type, "Cannot create instance for 'null' type.");

			Constructor<CommandOutput> constructor = CONSTRUCTORS.get(type);

			if (constructor == null) {
				constructor = (Constructor<CommandOutput>) ClassUtils.getConstructorIfAvailable(type, RedisCodec.class);
				CONSTRUCTORS.put(type, constructor);
			}

			if (constructor == null) {
				throw new IllegalArgumentException(
						"Cannot instantiate command output for type '%s'. No constructor accepting RedisCodec.".formatted(type));
			}

			return BeanUtils.instantiateClass(constructor, CODEC);
		}
	}

	/**
	 * Strategy interface to control pipelining flush behavior. Lettuce writes (flushes) each command individually to the
	 * Redis connection. Flushing behavior can be customized to optimize for performance. Flushing can be either stateless
	 * or stateful. An example for stateful flushing is size-based (buffer) flushing to flush after a configured number of
	 * commands.
	 *
	 * @see StatefulRedisConnection#setAutoFlushCommands(boolean)
	 * @see StatefulRedisConnection#flushCommands()
	 * @author Mark Paluch
	 * @since 2.3
	 */
	public interface PipeliningFlushPolicy {

		/**
		 * Return a policy to flush after each command (default behavior).
		 *
		 * @return a policy to flush after each command.
		 */
		static PipeliningFlushPolicy flushEachCommand() {
			return FlushEachCommand.INSTANCE;
		}

		/**
		 * Return a policy to flush only if {@link #closePipeline()} is called.
		 *
		 * @return a policy to flush after each command.
		 */
		static PipeliningFlushPolicy flushOnClose() {
			return FlushOnClose.INSTANCE;
		}

		/**
		 * Return a policy to buffer commands and to flush once reaching the configured {@code bufferSize}. The buffer is
		 * recurring so a buffer size of e.g. {@code 2} will flush after 2, 4, 6, … commands.
		 *
		 * @param bufferSize the number of commands to buffer before flushing. Must be greater than zero.
		 * @return a policy to flush buffered commands to the Redis connection once the configured number of commands was
		 *         issued.
		 */
		static PipeliningFlushPolicy buffered(int bufferSize) {

			Assert.isTrue(bufferSize > 0, "Buffer size must be greater than 0");
			return () -> new BufferedFlushing(bufferSize);
		}

		PipeliningFlushState newPipeline();
	}

	/**
	 * State object associated with flushing of the currently ongoing pipeline.
	 *
	 * @author Mark Paluch
	 * @since 2.3
	 */
	public interface PipeliningFlushState {

		/**
		 * Callback if the pipeline gets opened.
		 *
		 * @param connection Lettuce {@link StatefulConnection}.
		 * @see #openPipeline()
		 */
		void onOpen(StatefulConnection<?, ?> connection);

		/**
		 * Callback for each issued Redis command.
		 *
		 * @param connection Lettuce {@link StatefulConnection}.
		 * @see #pipeline(LettuceResult)
		 */
		void onCommand(StatefulConnection<?, ?> connection);

		/**
		 * Callback if the pipeline gets closed.
		 *
		 * @param connection Lettuce {@link StatefulConnection}.
		 * @see #closePipeline()
		 */
		void onClose(StatefulConnection<?, ?> connection);
	}

	/**
	 * Implementation to flush on each command.
	 *
	 * @author Mark Paluch
	 * @since 2.3
	 */
	private enum FlushEachCommand implements PipeliningFlushPolicy, PipeliningFlushState {

		INSTANCE;

		@Override
		public PipeliningFlushState newPipeline() {
			return INSTANCE;
		}

		@Override
		public void onOpen(StatefulConnection<?, ?> connection) {}

		@Override
		public void onCommand(StatefulConnection<?, ?> connection) {}

		@Override
		public void onClose(StatefulConnection<?, ?> connection) {}
	}

	/**
	 * Implementation to flush on closing the pipeline.
	 *
	 * @author Mark Paluch
	 * @since 2.3
	 */
	private enum FlushOnClose implements PipeliningFlushPolicy, PipeliningFlushState {

		INSTANCE;

		@Override
		public PipeliningFlushState newPipeline() {
			return INSTANCE;
		}

		@Override
		public void onOpen(StatefulConnection<?, ?> connection) {
			connection.setAutoFlushCommands(false);
		}

		@Override
		public void onCommand(StatefulConnection<?, ?> connection) {

		}

		@Override
		public void onClose(StatefulConnection<?, ?> connection) {
			connection.flushCommands();
			connection.setAutoFlushCommands(true);
		}
	}

	/**
	 * Pipeline state for buffered flushing.
	 *
	 * @author Mark Paluch
	 * @since 2.3
	 */
	private static class BufferedFlushing implements PipeliningFlushState {

		private final AtomicLong commands = new AtomicLong();

		private final int flushAfter;

		public BufferedFlushing(int flushAfter) {
			this.flushAfter = flushAfter;
		}

		@Override
		public void onOpen(StatefulConnection<?, ?> connection) {
			connection.setAutoFlushCommands(false);
		}

		@Override
		public void onCommand(StatefulConnection<?, ?> connection) {
			if (commands.incrementAndGet() % flushAfter == 0) {
				connection.flushCommands();
			}
		}

		@Override
		public void onClose(StatefulConnection<?, ?> connection) {
			connection.flushCommands();
			connection.setAutoFlushCommands(true);
		}
	}

	/**
	 * @since 2.3.8
	 */
	record CustomCommandType(String name) implements ProtocolKeyword {

		@Override
		public byte[] getBytes() {
			return name().getBytes(StandardCharsets.US_ASCII);
		}

		@Override
		public String name() {
			return this.name;
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof CustomCommandType that)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.name(), that.name());
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(name());
		}

		@Override
		public String toString() {
			return name();
		}
	}
}
