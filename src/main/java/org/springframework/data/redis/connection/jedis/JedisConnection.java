/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.connection.jedis.JedisInvoker.ResponseCommands;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisResultBuilder;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisStatusResult;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/redis/jedis">Jedis</a> library.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Jungtaek Lim
 * @author Konstantin Shchepanovskyi
 * @author David Liu
 * @author Milan Agatonovic
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Guy Korland
 * @author Dengliming
 * @author John Blum
 * @author Tihomir Mateev
 * @see redis.clients.jedis.Jedis
 * @see redis.clients.jedis.RedisClient
 */
@NullUnmarked
public class JedisConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private boolean convertPipelineAndTxResults = true;

	private final UnifiedJedis jedis;

	private final JedisClientConfig sentinelConfig;

	private final JedisInvoker invoker = new JedisInvoker((directFunction, pipelineFunction, converter,
			nullDefault) -> doInvoke(false, directFunction, pipelineFunction, converter, nullDefault));

	private final JedisInvoker statusInvoker = new JedisInvoker((directFunction, pipelineFunction, converter,
			nullDefault) -> doInvoke(true, directFunction, pipelineFunction, converter, nullDefault));

	private volatile @Nullable JedisSubscription subscription;

	private final JedisGeoCommands geoCommands = new JedisGeoCommands(this);
	private final JedisHashCommands hashCommands = new JedisHashCommands(this);
	private final JedisHyperLogLogCommands hllCommands = new JedisHyperLogLogCommands(this);
	private final JedisKeyCommands keyCommands = new JedisKeyCommands(this);
	private final JedisListCommands listCommands = new JedisListCommands(this);
	private final JedisScriptingCommands scriptingCommands = new JedisScriptingCommands(this);
	private final JedisServerCommands serverCommands = new JedisServerCommands(this);
	private final JedisSetCommands setCommands = new JedisSetCommands(this);
	private final JedisStreamCommands streamCommands = new JedisStreamCommands(this);
	private final JedisStringCommands stringCommands = new JedisStringCommands(this);
	private final JedisZSetCommands zSetCommands = new JedisZSetCommands(this);
	private final JedisJsonCommands jsonCommands = new JedisJsonCommands(this);

	private final Log LOGGER = LogFactory.getLog(getClass());

	private final JedisDelegateSupport delegate;

	@SuppressWarnings("rawtypes") private List<JedisResult> pipelinedResults = new ArrayList<>();

	private final @Nullable Pool<Jedis> pool;

	private Queue<FutureResult<Response<?>>> txResults = new LinkedList<>();

	private boolean closed;

	/**
	 * Constructs a new {@link JedisConnection}.
	 *
	 * @param jedis {@link Jedis} client.
	 * @deprecated since 4.1, for removal; use {@link #JedisConnection(UnifiedJedis)} instead.
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	public JedisConnection(@NonNull Jedis jedis) {
		this(jedis, null, 0);
	}

	/**
	 * Constructs a new {@link JedisConnection} backed by a Jedis {@link Pool}.
	 *
	 * @param jedis {@link Jedis} client.
	 * @param pool {@link Pool} of Redis connections; can be null, if no pool is used.
	 * @param dbIndex {@link Integer index} of the Redis database to use.
	 * @deprecated since 4.1, for removal; use {@link #JedisConnection(UnifiedJedis)} instead.
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	public JedisConnection(@NonNull Jedis jedis, @Nullable Pool<Jedis> pool, int dbIndex) {
		this(jedis, pool, dbIndex, null);
	}

	/**
	 * Constructs a new {@link JedisConnection} backed by a Jedis {@link Pool}.
	 *
	 * @param jedis {@link Jedis} client.
	 * @param pool {@link Pool} of Redis connections; can be null, if no pool is used.
	 * @param dbIndex {@link Integer index} of the Redis database to use.
	 * @param clientName {@link String name} given to this client; can be {@literal null}.
	 * @since 1.8
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	protected JedisConnection(@NonNull Jedis jedis, @Nullable Pool<Jedis> pool, int dbIndex,
			@Nullable String clientName) {
		this(jedis, pool, createConfig(dbIndex, clientName), createConfig(dbIndex, clientName));
	}

	/**
	 * Constructs a new {@link JedisConnection} backed by a Jedis {@link Pool}.
	 *
	 * @param jedis {@link Jedis} client.
	 * @param pool {@link Pool} of Redis connections; can be null, if no pool is used.
	 * @param nodeConfig {@literal Redis Node} configuration
	 * @param sentinelConfig {@literal Redis Sentinel} configuration
	 * @since 2.5
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	protected JedisConnection(@NonNull Jedis jedis, @Nullable Pool<Jedis> pool, @NonNull JedisClientConfig nodeConfig,
			@NonNull JedisClientConfig sentinelConfig) {

		this.jedis = new UnifiedJedisAdapter(jedis);
		this.delegate = new JedisDelegate(jedis);
		this.pool = pool;
		this.sentinelConfig = sentinelConfig;

		// select the db
		// if this fail, do manual clean-up before propagating the exception
		// as we're inside the constructor
		if (nodeConfig.getDatabase() != jedis.getDB()) {
			try {
				select(nodeConfig.getDatabase());
			} catch (DataAccessException ex) {
				close();
				throw ex;
			}
		}
	}

	/**
	 * Constructs a new {@link JedisConnection} backed by a Jedis {@link UnifiedJedis} client.
	 *
	 * @param jedis {@link UnifiedJedis} client.
	 * @since 4.1
	 */
	public JedisConnection(@NonNull UnifiedJedis jedis) {

		Assert.notNull(jedis, "UnifiedJedis must not be null");

		this.jedis = jedis;
		this.delegate = new UnifiedJedisDelegate();
		this.pool = null;
		this.sentinelConfig = DefaultJedisClientConfig.builder().build();
	}

	private static DefaultJedisClientConfig createConfig(int dbIndex, @Nullable String clientName) {
		return DefaultJedisClientConfig.builder().database(dbIndex).clientName(clientName).build();
	}

	private @Nullable Object doInvoke(boolean status, Function<UnifiedJedis, Object> directFunction,
			Function<ResponseCommands, Response<Object>> pipelineFunction, Converter<Object, Object> converter,
			Supplier<Object> nullDefault) {

		return doWithJedis(it -> {

			if (isQueueing()) {

				Response<Object> response = pipelineFunction.apply(JedisInvoker.createCommands(getRequiredTransaction()));
				transaction(status ? newStatusResult(response) : newJedisResult(response, converter, nullDefault));
				return null;
			}

			if (isPipelined()) {

				Response<Object> response = pipelineFunction.apply(JedisInvoker.createCommands(getRequiredPipeline()));
				pipeline(status ? newStatusResult(response) : newJedisResult(response, converter, nullDefault));
				return null;
			}

			Object result = directFunction.apply(getJedis());

			if (result == null) {
				return nullDefault.get();
			}

			return converter.convert(result);
		});
	}

	protected DataAccessException convertJedisAccessException(Exception cause) {
		DataAccessException exception = EXCEPTION_TRANSLATION.translate(cause);
		return exception != null ? exception : new RedisSystemException(cause.getMessage(), cause);
	}

	@Override
	public RedisCommands commands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return geoCommands;
	}

	@Override
	public RedisHashCommands hashCommands() {
		return hashCommands;
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return hllCommands;
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return keyCommands;
	}

	@Override
	public RedisListCommands listCommands() {
		return listCommands;
	}

	@Override
	public RedisSetCommands setCommands() {
		return setCommands;
	}

	@Override
	public RedisStreamCommands streamCommands() {
		return streamCommands;
	}

	@Override
	public RedisStringCommands stringCommands() {
		return stringCommands;
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return zSetCommands;
	}

	@Override
	public RedisJsonCommands jsonCommands() {
		return jsonCommands;
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return scriptingCommands;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return serverCommands;
	}

	@Override
	public Object execute(@NonNull String command, byte @NonNull []... args) {

		Assert.hasText(command, "A valid command needs to be specified");
		Assert.notNull(args, "Arguments must not be null");

		return doWithJedis(it -> {

			ProtocolCommand protocolCommand = () -> JedisConverters.toBytes(command);

			if (isQueueing() || isPipelined()) {

				CommandArguments arguments = new CommandArguments(protocolCommand).addObjects(args);
				CommandObject<Object> commandObject = new CommandObject<>(arguments, BuilderFactory.RAW_OBJECT);

				if (isPipelined()) {
					pipeline(newJedisResult(getRequiredPipeline().executeCommand(commandObject)));
				} else {
					transaction(newJedisResult(getRequiredTransaction().executeCommand(commandObject)));
				}
				return null;
			}

			return it.sendCommand(protocolCommand, args);
		});
	}

	@Override
	public void close() throws DataAccessException {

		super.close();

		JedisSubscription subscription = this.subscription;

		if (subscription != null) {
			doExceptionThrowingOperationSafely(subscription::close, "Cannot terminate subscription");
			this.subscription = null;
		}

		doClose();
	}

	/**
	 * Performs the actual close operation. Can be overridden by subclasses to customize close behavior.
	 */
	protected void doClose() {
		this.delegate.close();
		this.closed = true;
	}

	@Override
	public Object getNativeConnection() {
		if (this.jedis instanceof UnifiedJedisAdapter adapter) {
			return adapter.getJedis();
		}
		return this.jedis;
	}

	@Override
	public boolean isClosed() {

		if (this.jedis instanceof UnifiedJedisAdapter adapter) {
			return !adapter.getJedis().isConnected();
		}

		return closed;
	}

	@Override
	public boolean isQueueing() {
		return this.delegate.isQueueing();
	}

	@Override
	public boolean isPipelined() {
		return this.delegate.isPipelined();
	}

	@Override
	public void openPipeline() {
		this.delegate.openPipeline();
	}

	@Override
	public List<@Nullable Object> closePipeline() {
		return this.delegate.closePipeline();
	}

	private List<@Nullable Object> convertPipelineResults(AbstractPipeline pipeline) {

		List<Object> results = new ArrayList<>();

		pipeline.sync();

		Exception cause = null;

		for (JedisResult<?, ?> result : pipelinedResults) {
			try {

				Object data = result.get();

				if (!result.isStatus()) {
					results.add(result.conversionRequired() ? result.convert(data) : data);
				}
			} catch (JedisDataException ex) {
				DataAccessException dataAccessException = convertJedisAccessException(ex);
				if (cause == null) {
					cause = dataAccessException;
				}
				results.add(dataAccessException);
			} catch (DataAccessException ex) {
				if (cause == null) {
					cause = ex;
				}
				results.add(ex);
			}
		}

		if (cause != null) {
			throw new RedisPipelineException(cause, results);
		}

		return results;
	}

	void pipeline(@NonNull JedisResult<?, ?> result) {

		if (isQueueing()) {
			transaction(result);
		} else {
			pipelinedResults.add(result);
		}
	}

	void transaction(@NonNull FutureResult<Response<?>> result) {
		txResults.add(result);
	}

	@Override
	public byte[] echo(byte @NonNull [] message) {

		Assert.notNull(message, "Message must not be null");

		return invoke().from(jedis -> jedis.sendCommand(Protocol.Command.ECHO, message))
				.get(response -> (byte[]) response);
	}

	@Override
	public String ping() {
		return invoke().just(UnifiedJedis::ping);
	}

	@Override
	public void discard() {
		this.delegate.discard();
	}

	@Override
	public List<@Nullable Object> exec() {
		return this.delegate.exec();
	}

	public @Nullable AbstractPipeline getPipeline() {
		return this.delegate.getPipeline();
	}

	public AbstractPipeline getRequiredPipeline() {
		return this.delegate.getRequiredPipeline();
	}

	public @Nullable AbstractTransaction getTransaction() {
		return this.delegate.getTransaction();
	}

	public AbstractTransaction getRequiredTransaction() {
		return this.delegate.getRequiredTransaction();
	}

	/**
	 * Returns the underlying {@link UnifiedJedis} instance.
	 *
	 * @return the {@link UnifiedJedis} instance
	 */
	@NonNull
	public UnifiedJedis getJedis() {
		return this.jedis;
	}

	/**
	 * Obtain a {@link JedisInvoker} to call Jedis methods on the current {@link Jedis} instance.
	 *
	 * @return the {@link JedisInvoker}.
	 * @since 2.5
	 */
	@NonNull
	JedisInvoker invoke() {
		return this.invoker;
	}

	/**
	 * Obtain a {@link JedisInvoker} to call Jedis methods returning a status response on the current {@link Jedis}
	 * instance. Status responses are not included in transactional and pipeline results.
	 *
	 * @return the {@link JedisInvoker}.
	 * @since 2.5
	 */
	@NonNull
	JedisInvoker invokeStatus() {
		return this.statusInvoker;
	}

	<T> JedisResult<T, T> newJedisResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).build();
	}

	<T, R> JedisResult<T, R> newJedisResult(Response<T> response, Converter<T, R> converter, Supplier<R> defaultValue) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).mapNullTo(defaultValue).build();
	}

	<T> JedisStatusResult<T, T> newStatusResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).buildStatusResult();
	}

	@Override
	public void multi() {
		this.delegate.multi();
	}

	@Override
	public void select(int dbIndex) {
		this.delegate.select(dbIndex);
	}

	@Override
	public void unwatch() {
		this.delegate.unwatch();

	}

	@Override
	public void watch(byte @NonNull [] @NonNull... keys) {
		this.delegate.watch(keys);
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte @NonNull [] channel, byte @NonNull [] message) {
		return invoke().just(jedis -> jedis.publish(channel, message));
	}

	@Override
	public Subscription getSubscription() {
		return this.subscription;
	}

	@Override
	public boolean isSubscribed() {
		Subscription subscription = getSubscription();
		return subscription != null && subscription.isAlive();
	}

	@Override
	public void pSubscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... patterns) {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot subscribe in pipeline / transaction mode");
		}

		doWithJedis(it -> {

			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			it.psubscribe(jedisPubSub, patterns);
		});
	}

	@Override
	public void subscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... channels) {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot subscribe in pipeline / transaction mode");
		}

		doWithJedis(it -> {

			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			it.subscribe(jedisPubSub, channels);
		});
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Jedis driver
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	@Override
	protected boolean isActive(@NonNull RedisNode node) {

		Jedis verification = null;

		try {
			verification = getJedis(node);
			verification.connect();
			return verification.ping().equalsIgnoreCase("pong");
		} catch (Exception ignore) {
			return false;
		} finally {
			if (verification != null) {
				verification.disconnect();
				verification.close();
			}
		}
	}

	@Override
	protected JedisSentinelConnection getSentinelConnection(@NonNull RedisNode sentinel) {
		return new JedisSentinelConnection(getJedis(sentinel));
	}

	protected Jedis getJedis(@NonNull RedisNode node) {
		return new Jedis(JedisConverters.toHostAndPort(node), this.sentinelConfig);
	}

	private @Nullable <T> T doWithJedis(@NonNull Function<@NonNull UnifiedJedis, T> callback) {

		try {
			return callback.apply(getJedis());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doWithJedis(@NonNull Consumer<@NonNull UnifiedJedis> callback) {

		try {
			callback.accept(getJedis());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doExceptionThrowingOperationSafely(ExceptionThrowingOperation operation, String logMessage) {

		try {
			operation.run();
		} catch (Exception ex) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(logMessage, ex);
			}
		}
	}

	@FunctionalInterface
	private interface ExceptionThrowingOperation {
		void run() throws Exception;
	}

	/**
	 * Delegate to support both {@link Jedis} and {@link UnifiedJedis}.
	 *
	 * @since 4.1
	 */
	abstract class JedisDelegateSupport {

		private volatile @Nullable AbstractPipeline pipeline;

		private volatile @Nullable AbstractTransaction transaction;

		/**
		 * Select the database.
		 */
		public abstract void select(int dbIndex);

		/**
		 * Set the client name.
		 */
		public abstract void setClientName(byte @NonNull [] name);

		/**
		 * Watch keys in preparation for a conditional transaction.
		 */
		public abstract void watch(byte @NonNull [] @NonNull... keys);

		/**
		 * Unwatch the previously watched keys.
		 */
		public abstract void unwatch();

		/**
		 * Switch the connection to a transactional state.
		 */
		public abstract void multi();

		/**
		 * Discard the current transaction.
		 */
		public abstract void discard();

		/**
		 * Discard the given transaction.
		 */
		public void discard(AbstractTransaction transaction) {
			try {
				transaction.discard();
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			} finally {
				this.transaction = null;
				txResults.clear();
			}
		}

		/**
		 * Execute the current transaction and return the results.
		 */
		public abstract List<@Nullable Object> exec();

		/**
		 * Execute the current transaction and return the results.
		 */
		protected List<@Nullable Object> exec(AbstractTransaction transaction) {

			try {
				List<Object> results = transaction.exec();

				return !CollectionUtils.isEmpty(results)
						? new TransactionResultConverter<>(txResults, JedisExceptionConverter.INSTANCE).convert(results)
						: results;

			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			} finally {
				setTransaction(null);
				txResults.clear();
			}
		}

		/**
		 * Open a new pipeline.
		 */
		public abstract void openPipeline();

		/**
		 * Close the current pipeline and return the results. Returns {@link Collections#emptyList()} if no pipeline is
		 * active.
		 */
		public abstract List<@Nullable Object> closePipeline();

		/**
		 * Close the current pipeline and return the results. Returns {@link Collections#emptyList()} if no pipeline is
		 */
		protected List<@Nullable Object> closePipeline(AbstractPipeline pipeline) {

			try {
				return convertPipelineResults(pipeline);
			} finally {
				pipelinedResults.clear();
				setPipeline(null);
			}
		}

		/**
		 * Indicates whether the connection is in queue (or "MULTI") mode or not.
		 */
		public boolean isQueueing() {
			return getTransaction() != null;
		}

		/**
		 * Indicates whether the connection is currently pipelined or not.
		 */
		public boolean isPipelined() {
			return getPipeline() != null;
		}

		public @Nullable AbstractPipeline getPipeline() {
			return this.pipeline;
		}

		void setPipeline(@Nullable AbstractPipeline pipeline) {
			this.pipeline = pipeline;
		}

		/**
		 * Returns the {@link AbstractPipeline} associated with the current connection or throws
		 * {@link IllegalStateException} if no pipeline is active.
		 *
		 * @throws IllegalStateException if there is no active pipeline for the current connection.
		 */
		public AbstractPipeline getRequiredPipeline() {
			AbstractPipeline pipeline = getPipeline();
			Assert.state(pipeline != null, "Connection has no active pipeline");
			return pipeline;
		}

		public @Nullable AbstractTransaction getTransaction() {
			return this.transaction;
		}

		void setTransaction(@Nullable AbstractTransaction transaction) {
			this.transaction = transaction;
		}

		/**
		 * Returns the {@link AbstractTransaction} associated with the current connection or throws
		 * {@link IllegalStateException} if no transaction is active.
		 *
		 * @throws IllegalStateException if there is no active transaction for the current connection.
		 */
		public AbstractTransaction getRequiredTransaction() {
			AbstractTransaction transaction = getTransaction();
			Assert.state(transaction != null, "Connection has no active transaction");
			return transaction;
		}

		/**
		 * Close the connection state and clean up resources.
		 */
		public abstract void close();
	}

	/**
	 * Delegate for a {@link Jedis} instance.
	 */
	class JedisDelegate extends JedisDelegateSupport {

		private final Jedis jedis;

		public JedisDelegate(Jedis jedis) {
			this.jedis = jedis;
		}

		@Override
		public void select(int dbIndex) {
			jedis.select(dbIndex);
		}

		public void setClientName(byte @NonNull [] name) {
			jedis.clientSetname(name);
		}

		@Override
		public void watch(byte @NonNull [] @NonNull... keys) {

			if (isQueueing()) {
				throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
			}

			// compatibility mode - when using UnifiedJedis with a single connection we are safe to call watch directly
			this.jedis.watch(keys);
		}

		@Override
		public void unwatch() {
			this.jedis.unwatch();
		}

		@Override
		public void multi() {

			if (isQueueing()) {
				return;
			}

			if (isPipelined()) {
				throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
			}

			doWithJedis(jedis -> {
				setTransaction(jedis.multi());
			});
		}

		@Override
		public void discard() {
			super.discard(getRequiredTransaction());
		}

		@Override
		public List<@Nullable Object> exec() {

			if (!isQueueing()) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
			}

			return exec(getRequiredTransaction());
		}

		@Override
		public void openPipeline() {

			if (isQueueing()) {
				throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
			}

			if (!isPipelined()) {
				setPipeline(getJedis().pipelined());
			}
		}

		@Override
		public List<@Nullable Object> closePipeline() {

			if (isPipelined()) {
				return closePipeline(getRequiredPipeline());
			}

			return Collections.emptyList();
		}

		@Override
		public void close() {
			if (pool != null) {
				// Return connection to the pool or close directly
				jedis.close();
			} else {
				doExceptionThrowingOperationSafely(jedis::disconnect, "Failed to disconnect during close");
			}
		}
	}

	/**
	 * Delegate variant for a native {@link UnifiedJedis} instance.
	 */
	class UnifiedJedisDelegate extends JedisDelegateSupport {

		private boolean isMultiExecuted = false;

		/**
		 * Not supported with pooled connections. Configure the database via {@link JedisConnectionFactory} instead.
		 */
		@Override
		public void select(int dbIndex) {
			throw new InvalidDataAccessApiUsageException(
					"select(…) is not supported using UnifiedJedis. Select the database using JedisConnectionFactory");
		}

		/**
		 * Not supported with pooled connections. Configure the client name via
		 * {@link JedisClientConfiguration#getClientName()} instead.
		 */
		@Override
		public void setClientName(byte @NonNull [] name) {
			throw new InvalidDataAccessApiUsageException("setClientName is not supported with pooled connections. "
					+ "Configure the client name via JedisConnectionFactory.setClientName() or JedisClientConfig instead.");
		}

		/**
		 * Watches the given keys for modifications during a transaction. Binds to a dedicated connection from the pool to
		 * ensure WATCH, MULTI, and EXEC execute on the same connection.
		 */
		@Override
		public void watch(byte @NonNull [] @NonNull... keys) {

			if (isMultiExecuted()) {
				throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
			} else if (!isQueueing()) {
				setTransaction(getJedis().transaction(false));
			}

			getRequiredTransaction().watch(keys);
		}

		/**
		 * Unwatches all previously watched keys. Releases the dedicated connection back to the pool if MULTI was not yet
		 * called.
		 */
		@Override
		public void unwatch() {

			AbstractTransaction tx = getTransaction();

			if (tx != null) {
				try {
					tx.unwatch();
				} finally {
					// Only close if MULTI was not yet executed (still in WATCH-only state)
					if (!this.isMultiExecuted) {
						try {
							tx.close();
						} catch (Exception ignored) {
							// Ignore errors during close
						}
						setTransaction(null);
					}
				}
			}
		}

		/**
		 * Starts a Redis transaction. If {@link #watch(byte[]...)} was called previously, sends {@code MULTI} on the same
		 * dedicated connection. Otherwise, creates a new transaction.
		 */
		@Override
		public void multi() {

			if (isPipelined()) {
				throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
			}

			if (!isMultiExecuted()) {
				if (isQueueing()) {
					// watch was called previously and a transaction is already in progress
					this.getRequiredTransaction().multi();
					this.isMultiExecuted = true;
				} else {
					// pristine connection, start a new transaction
					setTransaction(jedis.multi());
					this.isMultiExecuted = true;
				}
			}
		}

		/**
		 * Executes all queued commands in the transaction and returns the connection to the pool.
		 *
		 * @return list of command results, or {@literal null} if the transaction was aborted
		 * @throws InvalidDataAccessApiUsageException if no transaction is active
		 */
		@Override
		public List<@Nullable Object> exec() {

			if (!isQueueing()) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
			}

			try (AbstractTransaction tx = getRequiredTransaction()) {
				return exec(tx);
			} finally {
				this.isMultiExecuted = false;
			}
		}

		/**
		 * Discards all queued commands and returns the connection to the pool.
		 *
		 * @throws InvalidDataAccessApiUsageException if no transaction is active
		 */
		@Override
		public void discard() {

			try (AbstractTransaction tx = getRequiredTransaction()) {
				super.discard(tx);
			} finally {
				this.isMultiExecuted = false;
			}
		}

		@Override
		public void openPipeline() {

			if (isQueueing()) {
				throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
			}

			if (!isPipelined()) {
				setPipeline(getJedis().pipelined());
			}
		}

		/**
		 * Closes the pipeline and returns the connection to the pool.
		 *
		 * @return list of pipeline command results
		 */
		@Override
		public List<@Nullable Object> closePipeline() {

			if (isPipelined()) {
				try (AbstractPipeline pipeline = getRequiredPipeline()) {
					return super.closePipeline(pipeline);
				}
			}

			return Collections.emptyList();
		}

		@Override
		public void close() {

			// Clean up any open pipeline to return connection to the pool
			if (isPipelined()) {
				try (AbstractPipeline currentPipeline = JedisConnection.this.getRequiredPipeline()) {}
				setPipeline(null);
			}

			// Clean up any open transaction to return connection to the pool
			try (AbstractTransaction currentTransaction = getRequiredTransaction()) {
				// Try to discard first to cleanly end the transaction
				currentTransaction.discard();
			} catch (Exception ignored) {
				// Transaction might not be in a state that allows discard
			}
			setTransaction(null);
			this.isMultiExecuted = false;
		}

		private boolean isMultiExecuted() {
			return isQueueing() && this.isMultiExecuted;
		}

	}

}
