/*
 * Copyright 2011-2022 the original author or authors.
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

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.commands.ServerCommands;
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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/xetorthio/jedis">Jedis</a> library.
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
 */
public class JedisConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private final Jedis jedis;

	private final JedisInvoker invoker = new JedisInvoker((directFunction, pipelineFunction, converter,
			nullDefault) -> doInvoke(false, directFunction, pipelineFunction, converter, nullDefault));
	private final JedisInvoker statusInvoker = new JedisInvoker((directFunction, pipelineFunction, converter,
			nullDefault) -> doInvoke(true, directFunction, pipelineFunction, converter, nullDefault));

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

	private final @Nullable Pool<Jedis> pool;
	private final JedisClientConfig sentinelConfig;


	private List<JedisResult> pipelinedResults = new ArrayList<>();
	private Queue<FutureResult<Response<?>>> txResults = new LinkedList<>();

	private volatile @Nullable JedisSubscription subscription;
	private volatile @Nullable Transaction transaction;
	private volatile @Nullable Pipeline pipeline;

	private boolean convertPipelineAndTxResults = true;

	/**
	 * Constructs a new <code>JedisConnection</code> instance.
	 *
	 * @param jedis Jedis entity
	 */
	public JedisConnection(Jedis jedis) {
		this(jedis, null, 0);
	}

	/**
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 * @param dbIndex
	 */
	public JedisConnection(Jedis jedis, Pool<Jedis> pool, int dbIndex) {
		this(jedis, pool, dbIndex, null);
	}

	/**
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 * @param dbIndex
	 * @param clientName the client name, can be {@literal null}.
	 * @since 1.8
	 */
	protected JedisConnection(Jedis jedis, @Nullable Pool<Jedis> pool, int dbIndex, @Nullable String clientName) {
		this(jedis, pool, createConfig(dbIndex, clientName), createConfig(dbIndex, clientName));
	}

	private static DefaultJedisClientConfig createConfig(int dbIndex, @Nullable String clientName) {
		return DefaultJedisClientConfig.builder().database(dbIndex).clientName(clientName).build();
	}

	/**
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 * @param nodeConfig node configuration
	 * @param sentinelConfig sentinel configuration
	 * @since 2.5
	 */
	protected JedisConnection(Jedis jedis, @Nullable Pool<Jedis> pool, JedisClientConfig nodeConfig,
			JedisClientConfig sentinelConfig) {

		this.jedis = jedis;
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

	@Nullable
	private Object doInvoke(boolean status, Function<Jedis, Object> directFunction,
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

	protected DataAccessException convertJedisAccessException(Exception ex) {
		DataAccessException exception = EXCEPTION_TRANSLATION.translate(ex);
		return exception != null ? exception : new RedisSystemException(ex.getMessage(), ex);
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
	public RedisScriptingCommands scriptingCommands() {
		return scriptingCommands;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return serverCommands;
	}

	@Override
	public Object execute(String command, byte[]... args) {


		Assert.hasText(command, "A valid command needs to be specified!");
		Assert.notNull(args, "Arguments must not be null!");

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

		// return the connection to the pool
		if (pool != null) {
			jedis.close();
			return;
		}
		// else close the connection normally (doing the try/catch dance)
		Exception exc = null;
		try {
			jedis.quit();
		} catch (Exception ex) {
			exc = ex;
		}
		try {
			jedis.disconnect();
		} catch (Exception ex) {
			exc = ex;
		}
		if (exc != null)
			throw convertJedisAccessException(exc);
	}

	@Override
	public Jedis getNativeConnection() {
		return jedis;
	}

	@Override
	public boolean isClosed() {
		return doWithJedis(it -> !it.isConnected());
	}

	@Override
	public boolean isQueueing() {
		return transaction != null;
	}

	@Override
	public boolean isPipelined() {
		return pipeline != null;
	}

	@Override
	public void openPipeline() {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
		}

		if (pipeline == null) {
			pipeline = jedis.pipelined();
		}
	}

	@Override
	public List<Object> closePipeline() {
		if (pipeline != null) {
			try {
				return convertPipelineResults();
			} finally {
				pipeline = null;
				pipelinedResults.clear();
			}
		}
		return Collections.emptyList();
	}

	private List<Object> convertPipelineResults() {
		List<Object> results = new ArrayList<>();
		getRequiredPipeline().sync();
		Exception cause = null;
		for (JedisResult result : pipelinedResults) {
			try {

				Object data = result.get();

				if (!result.isStatus()) {
					results.add(result.conversionRequired() ? result.convert(data) : data);
				}
			} catch (JedisDataException e) {
				DataAccessException dataAccessException = convertJedisAccessException(e);
				if (cause == null) {
					cause = dataAccessException;
				}
				results.add(dataAccessException);
			} catch (DataAccessException e) {
				if (cause == null) {
					cause = e;
				}
				results.add(e);
			}
		}
		if (cause != null) {
			throw new RedisPipelineException(cause, results);
		}
		return results;
	}

	void pipeline(JedisResult result) {
		if (isQueueing()) {
			transaction(result);
		} else {
			pipelinedResults.add(result);
		}
	}

	void transaction(FutureResult<Response<?>> result) {
		txResults.add(result);
	}

	@Override
	public byte[] echo(byte[] message) {

		Assert.notNull(message, "Message must not be null");

		return invoke().just(j -> j.echo(message));
	}

	@Override
	public String ping() {
		return invoke().just(ServerCommands::ping);
	}

	@Override
	public void discard() {
		try {
			getRequiredTransaction().discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Override
	public List<Object> exec() {
		try {

			if (transaction == null) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction. Did you forget to call multi?");
			}

			List<Object> results = transaction.exec();

			return !CollectionUtils.isEmpty(results)
					? new TransactionResultConverter<>(txResults, JedisExceptionConverter.INSTANCE).convert(results)
					: results;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Nullable
	public Pipeline getPipeline() {
		return pipeline;
	}

	public Pipeline getRequiredPipeline() {

		Pipeline pipeline = getPipeline();

		if (pipeline == null) {
			throw new IllegalStateException("Connection has no active pipeline");
		}

		return pipeline;
	}

	@Nullable
	public Transaction getTransaction() {
		return transaction;
	}

	public Transaction getRequiredTransaction() {

		Transaction transaction = getTransaction();

		if (transaction == null) {
			throw new IllegalStateException("Connection has no active transaction");
		}

		return transaction;
	}

	public Jedis getJedis() {
		return jedis;
	}

	/**
	 * Obtain a {@link JedisInvoker} to call Jedis methods on the current {@link Jedis} instance.
	 *
	 * @return the {@link JedisInvoker}.
	 * @since 2.5
	 */
	JedisInvoker invoke() {
		return invoker;
	}

	/**
	 * Obtain a {@link JedisInvoker} to call Jedis methods returning a status response on the current {@link Jedis}
	 * instance. Status responses are not included in transactional and pipeline results.
	 *
	 * @return the {@link JedisInvoker}.
	 * @since 2.5
	 */
	JedisInvoker invokeStatus() {
		return statusInvoker;
	}

	<T> JedisResult<T, T> newJedisResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).build();
	}

	<T, R> JedisResult<T, R> newJedisResult(Response<T> response, Converter<T, R> converter) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).build();
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
		if (isQueueing()) {
			return;
		}

		if (isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
		}

		doWithJedis(it -> {
			this.transaction = it.multi();
		});
	}

	@Override
	public void select(int dbIndex) {
		getJedis().select(dbIndex);
	}

	@Override
	public void unwatch() {
		doWithJedis((Consumer<Jedis>) Jedis::unwatch);
	}

	@Override
	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		doWithJedis(it -> {

			for (byte[] key : keys) {
					it.watch(key);
			}
		});
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte[] channel, byte[] message) {
		return invoke().just(j -> j.publish(channel, message));
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

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException();
		}

		doWithJedis(it -> {

			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			it.psubscribe(jedisPubSub, patterns);
		});
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException();
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
	protected boolean isActive(RedisNode node) {

		Jedis verification = null;
		try {
			verification = getJedis(node);
			verification.connect();
			return verification.ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (verification != null) {
				verification.disconnect();
				verification.close();
			}
		}
	}

	@Override
	protected JedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		return new JedisSentinelConnection(getJedis(sentinel));
	}

	protected Jedis getJedis(RedisNode node) {
		return new Jedis(new HostAndPort(node.getHost(), node.getPort()), this.sentinelConfig);
	}

	@Nullable
	private <T> T doWithJedis(Function<Jedis, T> callback) {

		try {
			return callback.apply(getJedis());

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doWithJedis(Consumer<Jedis> callback) {

		try {
			callback.accept(getJedis());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

}
