/*
 * Copyright 2026-present the original author or authors.
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

import java.util.*;
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
import org.springframework.data.redis.connection.jedis.JedisResult.JedisResultBuilder;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisStatusResult;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import redis.clients.jedis.*;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/redis/jedis">Jedis</a> 7.2+ library
 * using the {@link UnifiedJedis} API.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 * <p>
 * Supports {@link UnifiedJedis} for standalone connections, {@link RedisSentinelClient} for sentinel-managed
 * connections, and other {@link UnifiedJedis} implementations.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see UnifiedJedis
 * @see RedisClient
 * @see RedisSentinelClient
 * @see JedisConnection
 */
@NullUnmarked
public class JedisClientConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private boolean convertPipelineAndTxResults = true;

	private final UnifiedJedis client;

	private volatile @Nullable JedisSubscription subscription;

	private final JedisClientGeoCommands geoCommands = new JedisClientGeoCommands(this);
	private final JedisClientHashCommands hashCommands = new JedisClientHashCommands(this);
	private final JedisClientHyperLogLogCommands hllCommands = new JedisClientHyperLogLogCommands(this);
	private final JedisClientKeyCommands keyCommands = new JedisClientKeyCommands(this);
	private final JedisClientListCommands listCommands = new JedisClientListCommands(this);
	private final JedisClientScriptingCommands scriptingCommands = new JedisClientScriptingCommands(this);
	private final JedisClientServerCommands serverCommands = new JedisClientServerCommands(this);
	private final JedisClientSetCommands setCommands = new JedisClientSetCommands(this);
	private final JedisClientStreamCommands streamCommands = new JedisClientStreamCommands(this);
	private final JedisClientStringCommands stringCommands = new JedisClientStringCommands(this);
	private final JedisClientZSetCommands zSetCommands = new JedisClientZSetCommands(this);

	private final Log log = LogFactory.getLog(getClass());

	@SuppressWarnings("rawtypes") private final List<JedisResult> pipelinedResults = new ArrayList<>();

	private final Queue<FutureResult<@NonNull Response<?>>> txResults = new LinkedList<>();

	private volatile @Nullable AbstractPipeline pipeline;

	private volatile @Nullable AbstractTransaction transaction;

	// Execution strategy - changes based on pipeline/transaction state
	private ExecutionStrategy executionStrategy = new DirectExecutionStrategy();

	public JedisClientConnection(@NonNull UnifiedJedis client) {
		this(client, DefaultJedisClientConfig.builder().build());
	}

	public JedisClientConnection(@NonNull UnifiedJedis client, int dbIndex) {
		this(client, dbIndex, null);
	}

	public JedisClientConnection(@NonNull UnifiedJedis client, int dbIndex, @Nullable String clientName) {
		this(client, createConfig(dbIndex, clientName));
	}

	public JedisClientConnection(@NonNull UnifiedJedis client, @NonNull JedisClientConfig clientConfig) {

		Assert.notNull(client, "UnifiedJedis client must not be null");
		Assert.notNull(clientConfig, "JedisClientConfig must not be null");

		this.client = client;

		// Select the configured database to ensure clean state
		// This matches the behavior of the legacy JedisConnection which always selects the database in the constructor
		// to ensure connections from the pool start with the expected database, regardless of what previous operations did
		select(clientConfig.getDatabase());
	}

	private static DefaultJedisClientConfig createConfig(int dbIndex, @Nullable String clientName) {
		return DefaultJedisClientConfig.builder().database(dbIndex).clientName(clientName).build();
	}

	/**
	 * Execute a Redis command with identity conversion (no transformation).
	 * <p>
	 * The {@code batchFunction} is used for both pipeline and transaction modes, as both {@link AbstractPipeline} and
	 * {@link AbstractTransaction} extend {@link PipeliningBase} and share the same API.
	 *
	 * @param directFunction function to execute in direct mode on UnifiedJedis
	 * @param batchFunction function to execute in pipeline or transaction mode on PipeliningBase
	 * @param <T> the result type
	 * @return the command result, or null in pipelined/transactional mode
	 */
	<T> @Nullable T execute(Function<UnifiedJedis, T> directFunction,
			Function<PipeliningBase, Response<T>> batchFunction) {
		return executionStrategy.execute(directFunction, batchFunction);
	}

	/**
	 * Execute a Redis command that returns a status response. Status responses are handled specially and not included in
	 * transactional results.
	 * <p>
	 * The {@code batchFunction} is used for both pipeline and transaction modes, as both {@link AbstractPipeline} and
	 * {@link AbstractTransaction} extend {@link PipeliningBase} and share the same command API.
	 *
	 * @param directFunction function to execute in direct mode on UnifiedJedis
	 * @param batchFunction function to execute in pipeline or transaction mode on PipeliningBase
	 * @param <T> the result type
	 * @return the command result, or null in pipelined/transactional mode
	 */
	<T> @Nullable T executeStatus(Function<UnifiedJedis, T> directFunction,
			Function<PipeliningBase, Response<T>> batchFunction) {
		return executionStrategy.executeStatus(directFunction, batchFunction);
	}

	/**
	 * Execute a Redis command with a custom converter.
	 * <p>
	 * The {@code batchFunction} is used for both pipeline and transaction modes, as both {@link AbstractPipeline} and
	 * {@link AbstractTransaction} extend {@link PipeliningBase} and share the same command API.
	 *
	 * @param directFunction function to execute in direct mode on UnifiedJedis
	 * @param batchFunction function to execute in pipeline or transaction mode on PipeliningBase
	 * @param converter converter to transform the result
	 * @param <S> the source type
	 * @param <T> the target type
	 * @return the converted command result, or null in pipelined/transactional mode
	 */
	<S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
			Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter) {

		return execute(directFunction, batchFunction, converter, () -> null);
	}

	/**
	 * Execute a Redis command with a custom converter and default value.
	 * <p>
	 * The {@code batchFunction} is used for both pipeline and transaction modes, as both {@link AbstractPipeline} and
	 * {@link AbstractTransaction} extend {@link PipeliningBase} and share the same command API.
	 *
	 * @param directFunction function to execute in direct mode on UnifiedJedis
	 * @param batchFunction function to execute in pipeline or transaction mode on PipeliningBase
	 * @param converter converter to transform the result
	 * @param defaultValue supplier for default value when result is null
	 * @param <S> the source type
	 * @param <T> the target type
	 * @return the converted command result, or null in pipelined/transactional mode
	 */
	<S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
			Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter,
			Supplier<T> defaultValue) {
		return executionStrategy.execute(directFunction, batchFunction, converter, defaultValue);
	}

	/**
	 * Converts Jedis exceptions to Spring's {@link DataAccessException} hierarchy.
	 *
	 * @param cause the exception to convert
	 * @return the converted {@link DataAccessException}
	 */
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
	public RedisScriptingCommands scriptingCommands() {
		return scriptingCommands;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return serverCommands;
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
	public Object execute(@NonNull String command, byte @NonNull []... args) {
		return execute(command, false, null, args);
	}

	/**
	 * Execute a command with optional converter and status flag.
	 *
	 * @param command the command to execute
	 * @param isStatus whether this is a status command (should not add results to pipeline)
	 * @param converter optional converter to transform the result
	 * @param args command arguments
	 * @return the result
	 */
	<T> @Nullable T execute(@NonNull String command, boolean isStatus, @Nullable Converter<Object, T> converter,
			byte @NonNull []... args) {

		Assert.hasText(command, "A valid command needs to be specified");
		Assert.notNull(args, "Arguments must not be null");

		return doWithClient(c -> {

			ProtocolCommand protocolCommand = () -> JedisConverters.toBytes(command);

			if (isQueueing() || isPipelined()) {

				CommandArguments arguments = new CommandArguments(protocolCommand).addObjects(args);
				CommandObject<Object> commandObject = new CommandObject<>(arguments, BuilderFactory.RAW_OBJECT);

				if (isPipelined()) {
					if (isStatus) {
						pipeline(newStatusResult(getRequiredPipeline().executeCommand(commandObject)));
					} else if (converter != null) {
						pipeline(newJedisResult(getRequiredPipeline().executeCommand(commandObject), converter, () -> null));
					} else {
						pipeline(newJedisResult(getRequiredPipeline().executeCommand(commandObject)));
					}
				} else {
					if (isStatus) {
						transaction(newStatusResult(getRequiredTransaction().executeCommand(commandObject)));
					} else if (converter != null) {
						transaction(newJedisResult(getRequiredTransaction().executeCommand(commandObject), converter, () -> null));
					} else {
						transaction(newJedisResult(getRequiredTransaction().executeCommand(commandObject)));
					}
				}
				return null;
			}

			Object result = c.sendCommand(protocolCommand, args);
			return converter != null ? converter.convert(result) : (T) result;
		});
	}

	@Override
	public void close() throws DataAccessException {

		super.close();

		JedisSubscription subscription = this.subscription;

		if (subscription != null) {
			doExceptionThrowingOperationSafely(subscription::close);
			this.subscription = null;
		}

		// Close any open pipeline to ensure connection is returned to pool
		if (isPipelined()) {
			try {
				closePipeline();
			} catch (Exception ex) {
				log.warn("Failed to close pipeline during connection close", ex);
			}
		}

		// Discard any open transaction
		if (isQueueing()) {
			try {
				discard();
			} catch (Exception ex) {
				log.warn("Failed to discard transaction during connection close", ex);
			}
		}

		// RedisClient is managed by the factory, so we don't close it here
	}

	@Override
	public UnifiedJedis getNativeConnection() {
		return this.client;
	}

	@Override
	public boolean isClosed() {
		// UnifiedJedis doesn't expose connection state directly
		// We rely on the factory to manage the lifecycle
		return false;
	}

	@Override
	public boolean isQueueing() {
		return this.transaction != null;
	}

	@Override
	public boolean isPipelined() {
		return this.pipeline != null;
	}

	@Override
	public void openPipeline() {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
		}

		if (pipeline == null) {
			pipeline = client.pipelined();
			executionStrategy = new PipelineExecutionStrategy();
		}
	}

	@Override
	public List<@Nullable Object> closePipeline() {

		if (pipeline != null) {
			try {
				return convertPipelineResults();
			} finally {
				try {
					pipeline.close(); // Return connection to pool
				} catch (Exception ex) {
					log.warn("Failed to close pipeline", ex);
				}
				pipeline = null;
				pipelinedResults.clear();
				executionStrategy = new DirectExecutionStrategy();
			}
		}

		return Collections.emptyList();
	}

	private List<@Nullable Object> convertPipelineResults() {

		List<Object> results = new ArrayList<>();

		getRequiredPipeline().sync();

		Exception cause = null;

		for (JedisResult<?, ?> result : pipelinedResults) {
			try {

				Object data = result.get();

				if (!result.isStatus()) {
					results.add(result.conversionRequired() ? result.convert(data) : data);
				}
			} catch (Exception ex) {
				DataAccessException dataAccessException = convertJedisAccessException(ex);
				if (cause == null) {
					cause = dataAccessException;
				}
				results.add(dataAccessException);
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

	void transaction(@NonNull FutureResult<@NonNull Response<?>> result) {
		txResults.add(result);
	}

	@Override
	public void select(int dbIndex) {
		doWithClient((Consumer<UnifiedJedis>) c -> c.sendCommand(Protocol.Command.SELECT, String.valueOf(dbIndex)));
	}

	@Override
	public byte[] echo(byte @NonNull [] message) {

		Assert.notNull(message, "Message must not be null");

		return execute(client -> (byte[]) client.sendCommand(Protocol.Command.ECHO, message),
				pipeline -> pipeline.sendCommand(Protocol.Command.ECHO, message), result -> (byte[]) result);
	}

	@Override
	public String ping() {
		return execute(UnifiedJedis::ping, pipeline -> pipeline.sendCommand(Protocol.Command.PING, new byte[0][]),
				result -> result instanceof byte[] ? JedisConverters.toString((byte[]) result) : (String) result);
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type.
	 *
	 * @param convertPipelineAndTxResults {@code true} to convert pipeline and transaction results.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	public @Nullable AbstractPipeline getPipeline() {
		return this.pipeline;
	}

	public AbstractPipeline getRequiredPipeline() {

		AbstractPipeline pipeline = getPipeline();

		Assert.state(pipeline != null, "Connection has no active pipeline");

		return pipeline;
	}

	public @Nullable AbstractTransaction getTransaction() {
		return this.transaction;
	}

	public AbstractTransaction getRequiredTransaction() {

		AbstractTransaction transaction = getTransaction();

		Assert.state(transaction != null, "Connection has no active transaction");

		return transaction;
	}

	/**
	 * Returns the underlying {@link UnifiedJedis} client instance.
	 * <p>
	 * This can be a {@link RedisClient}, {@link RedisSentinelClient}, or other {@link UnifiedJedis} implementation.
	 *
	 * @return the {@link UnifiedJedis} instance. Never {@literal null}.
	 */
	@NonNull
	public UnifiedJedis getRedisClient() {
		return this.client;
	}

	/**
	 * Returns the underlying {@link UnifiedJedis} client instance.
	 * <p>
	 * This method is used by SCAN operations in command classes. This can be a {@link RedisClient},
	 * {@link RedisSentinelClient}, or other {@link UnifiedJedis} implementation.
	 *
	 * @return the {@link UnifiedJedis} client. Never {@literal null}.
	 */
	@NonNull
	public UnifiedJedis getJedis() {
		return this.client;
	}

	<T> JedisResult<@NonNull T, @NonNull T> newJedisResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).convertPipelineAndTxResults(convertPipelineAndTxResults)
				.build();
	}

	<T, R> JedisResult<@NonNull T, @NonNull R> newJedisResult(Response<T> response, Converter<@NonNull T, R> converter,
			Supplier<R> defaultValue) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).mapNullTo(defaultValue).build();
	}

	<T> JedisStatusResult<@NonNull T, @NonNull T> newStatusResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).buildStatusResult();
	}

	@Override
	protected boolean isActive(@NonNull RedisNode node) {
		// Sentinel support not yet implemented
		return false;
	}

	@Override
	protected RedisSentinelConnection getSentinelConnection(@NonNull RedisNode sentinel) {
		throw new UnsupportedOperationException("Sentinel is not supported by JedisClientConnection");
	}

	private @Nullable <T> T doWithClient(@NonNull Function<@NonNull UnifiedJedis, T> callback) {

		try {
			return callback.apply(getRedisClient());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doWithClient(@NonNull Consumer<@NonNull UnifiedJedis> callback) {

		try {
			callback.accept(getRedisClient());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doExceptionThrowingOperationSafely(Runnable operation) {
		try {
			operation.run();
		} catch (Exception ex) {
			log.warn("Cannot terminate subscription", ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte @NonNull [] channel, byte @NonNull [] message) {
		return doWithClient((Function<UnifiedJedis, Long>) c -> c.publish(channel, message));
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public boolean isSubscribed() {
		return subscription != null && subscription.isAlive();
	}

	@Override
	public void subscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... channels) {

		if (isSubscribed()) {
			throw new InvalidDataAccessApiUsageException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			client.subscribe(jedisPubSub, channels);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSubscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... patterns) {

		if (isSubscribed()) {
			throw new InvalidDataAccessApiUsageException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			client.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// Transaction functionality
	//

	@Override
	public void multi() {

		if (isQueueing()) {
			return;
		}

		if (isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
		}

		doWithClient(c -> {
			this.transaction = c.multi();
			executionStrategy = new TransactionExecutionStrategy();
		});
	}

	@Override
	public List<@Nullable Object> exec() {

		try {
			if (transaction == null) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
			}

			List<Object> results = transaction.exec();
			return !CollectionUtils.isEmpty(results)
					? new TransactionResultConverter<>(txResults, JedisExceptionConverter.INSTANCE).convert(results)
					: results;

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			try {
				if (transaction != null) {
					transaction.close(); // Return connection to pool
				}
			} catch (Exception ex) {
				log.warn("Failed to close transaction", ex);
			}
			txResults.clear();
			transaction = null;
			executionStrategy = new DirectExecutionStrategy();
		}
	}

	@Override
	public void discard() {

		try {
			getRequiredTransaction().discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			try {
				if (transaction != null) {
					transaction.close(); // Return connection to pool
				}
			} catch (Exception ex) {
				log.warn("Failed to close transaction", ex);
			}
			txResults.clear();
			transaction = null;
			executionStrategy = new DirectExecutionStrategy();
		}
	}

	@Override
	public void watch(byte @NonNull [] @NonNull... keys) {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
		}

		doWithClient((Consumer<UnifiedJedis>) c -> c.sendCommand(Protocol.Command.WATCH, keys));
	}

	@Override
	public void unwatch() {
		doWithClient((Consumer<UnifiedJedis>) c -> c.sendCommand(Protocol.Command.UNWATCH));
	}

	/**
	 * Strategy interface for executing commands in different modes (direct, pipeline, transaction).
	 */
	private interface ExecutionStrategy {
		<T> @Nullable T execute(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction);

		<T> @Nullable T executeStatus(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction);

		<S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
				Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter,
				Supplier<T> defaultValue);
	}

	/**
	 * Direct execution strategy - executes commands immediately on UnifiedJedis.
	 */
	private final class DirectExecutionStrategy implements ExecutionStrategy {
		@Override
		public <T> @Nullable T execute(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			return doWithClient(directFunction);
		}

		@Override
		public <T> @Nullable T executeStatus(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			return doWithClient(directFunction);
		}

		@Override
		public <S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
				Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter,
				Supplier<T> defaultValue) {
			return doWithClient(c -> {
				S result = directFunction.apply(c);
				return result != null ? converter.convert(result) : defaultValue.get();
			});
		}
	}

	/**
	 * Pipeline execution strategy - queues commands in a pipeline.
	 */
	private final class PipelineExecutionStrategy implements ExecutionStrategy {
		@Override
		public <T> @Nullable T execute(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			Response<T> response = batchFunction.apply(getRequiredPipeline());
			pipeline(newJedisResult(response));
			return null;
		}

		@Override
		public <T> @Nullable T executeStatus(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			Response<T> response = batchFunction.apply(getRequiredPipeline());
			pipeline(newStatusResult(response));
			return null;
		}

		@Override
		public <S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
				Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter,
				Supplier<T> defaultValue) {
			Response<S> response = batchFunction.apply(getRequiredPipeline());
			pipeline(newJedisResult(response, converter, defaultValue));
			return null;
		}
	}

	/**
	 * Transaction execution strategy - queues commands in a transaction.
	 */
	private final class TransactionExecutionStrategy implements ExecutionStrategy {
		@Override
		public <T> @Nullable T execute(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			Response<T> response = batchFunction.apply(getRequiredTransaction());
			transaction(newJedisResult(response));
			return null;
		}

		@Override
		public <T> @Nullable T executeStatus(Function<UnifiedJedis, T> directFunction,
				Function<PipeliningBase, Response<T>> batchFunction) {
			Response<T> response = batchFunction.apply(getRequiredTransaction());
			transaction(newStatusResult(response));
			return null;
		}

		@Override
		public <S, T> @Nullable T execute(Function<UnifiedJedis, S> directFunction,
				Function<PipeliningBase, Response<S>> batchFunction, Converter<@NonNull S, T> converter,
				Supplier<T> defaultValue) {
			Response<S> response = batchFunction.apply(getRequiredTransaction());
			transaction(newJedisResult(response, converter, defaultValue));
			return null;
		}
	}
}
