/*
 * Copyright 2011-2017 the original author or authors.
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

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Builder;
import redis.clients.jedis.Client;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Queable;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Pool;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/xetorthio/jedis">Jedis</a> library.
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
 */
public class JedisConnection extends AbstractRedisConnection {

	private static final Field CLIENT_FIELD;
	private static final Method SEND_COMMAND;
	private static final Method GET_RESPONSE;

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	static {

		CLIENT_FIELD = ReflectionUtils.findField(BinaryJedis.class, "client", Client.class);
		ReflectionUtils.makeAccessible(CLIENT_FIELD);

		try {
			Class<?> commandType = ClassUtils.isPresent("redis.clients.jedis.ProtocolCommand", null)
					? ClassUtils.forName("redis.clients.jedis.ProtocolCommand", null)
					: ClassUtils.forName("redis.clients.jedis.Protocol$Command", null);

			SEND_COMMAND = ReflectionUtils.findMethod(Connection.class, "sendCommand",
					new Class[] { commandType, byte[][].class });
		} catch (Exception e) {
			throw new NoClassDefFoundError(
					"Could not find required flavor of command required by 'redis.clients.jedis.Connection#sendCommand'.");
		}

		ReflectionUtils.makeAccessible(SEND_COMMAND);

		GET_RESPONSE = ReflectionUtils.findMethod(Queable.class, "getResponse", Builder.class);
		ReflectionUtils.makeAccessible(GET_RESPONSE);
	}

	private final Jedis jedis;
	private final Client client;
	private Transaction transaction;
	private final Pool<Jedis> pool;
	/**
	 * flag indicating whether the connection needs to be dropped or not
	 */
	private boolean broken = false;
	private volatile JedisSubscription subscription;
	private volatile Pipeline pipeline;
	private final int dbIndex;
	private final String clientName;
	private boolean convertPipelineAndTxResults = true;
	private List<FutureResult<Response<?>>> pipelinedResults = new ArrayList<>();
	private Queue<FutureResult<Response<?>>> txResults = new LinkedList<>();

	class JedisResult extends FutureResult<Response<?>> {
		public <T> JedisResult(Response<T> resultHolder, Converter<T, ?> converter) {
			super(resultHolder, converter);
		}

		public <T> JedisResult(Response<T> resultHolder) {
			super(resultHolder);
		}

		@SuppressWarnings("unchecked")
		public Object get() {
			if (convertPipelineAndTxResults && converter != null) {
				return converter.convert(resultHolder.get());
			}
			return resultHolder.get();
		}
	}

	private class JedisStatusResult extends JedisResult {
		public JedisStatusResult(Response<?> resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

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
	protected JedisConnection(Jedis jedis, Pool<Jedis> pool, int dbIndex, String clientName) {

		// extract underlying connection for batch operations
		client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);

		this.jedis = jedis;
		this.pool = pool;
		this.dbIndex = dbIndex;
		this.clientName = clientName;

		// select the db
		// if this fail, do manual clean-up before propagating the exception
		// as we're inside the constructor
		if (dbIndex > 0) {
			try {
				select(dbIndex);
			} catch (DataAccessException ex) {
				close();
				throw ex;
			}
		}
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {

		if (ex instanceof NullPointerException) {
			// An NPE before flush will leave data in the OutputStream of a pooled connection
			broken = true;
		}

		DataAccessException exception = EXCEPTION_TRANSLATION.translate(ex);
		if (exception instanceof RedisConnectionFailureException) {
			broken = true;
		}

		return exception;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#keyCommands()
	 */
	@Override
	public RedisKeyCommands keyCommands() {
		return new JedisKeyCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#stringCommands()
	 */
	@Override
	public RedisStringCommands stringCommands() {
		return new JedisStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#listCommands()
	 */
	@Override
	public RedisListCommands listCommands() {
		return new JedisListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#setCommands()
	 */
	@Override
	public RedisSetCommands setCommands() {
		return new JedisSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#zSetCommands()
	 */
	@Override
	public RedisZSetCommands zSetCommands() {
		return new JedisZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hashCommands()
	 */
	@Override
	public RedisHashCommands hashCommands() {
		return new JedisHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#geoCommands()
	 */
	@Override
	public RedisGeoCommands geoCommands() {
		return new JedisGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#scriptingCommands()
	 */
	@Override
	public RedisScriptingCommands scriptingCommands() {
		return new JedisScriptingCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#serverCommands()
	 */
	@Override
	public RedisServerCommands serverCommands() {
		return new JedisServerCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hyperLogLogCommands()
	 */
	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return new JedisHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisCommands#execute(java.lang.String, byte[][])
	 */
	@Override
	public Object execute(String command, byte[]... args) {
		Assert.hasText(command, "a valid command needs to be specified");
		try {
			List<byte[]> mArgs = new ArrayList<>();
			if (!ObjectUtils.isEmpty(args)) {
				Collections.addAll(mArgs, args);
			}

			ReflectionUtils.invokeMethod(SEND_COMMAND, client, Command.valueOf(command.trim().toUpperCase()),
					mArgs.toArray(new byte[mArgs.size()][]));
			if (isQueueing() || isPipelined()) {
				Object target = (isPipelined() ? pipeline : transaction);
				@SuppressWarnings("unchecked")
				Response<Object> result = (Response<Object>) ReflectionUtils.invokeMethod(GET_RESPONSE, target,
						new Builder<Object>() {
							public Object build(Object data) {
								return data;
							}

							public String toString() {
								return "Object";
							}
						});
				if (isPipelined()) {
					pipeline(new JedisResult(result));
				} else {
					transaction(new JedisResult(result));
				}
				return null;
			}
			return client.getOne();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.AbstractRedisConnection#close()
	 */
	@Override
	public void close() throws DataAccessException {
		super.close();
		// return the connection to the pool
		if (pool != null) {
			if (!broken) {
				// reset the connection
				try {
					if (dbIndex > 0) {
						jedis.select(0);
					}
					pool.returnResource(jedis);
					return;
				} catch (Exception ex) {
					DataAccessException dae = convertJedisAccessException(ex);
					if (broken) {
						pool.returnBrokenResource(jedis);
					} else {
						pool.returnResource(jedis);
					}
					throw dae;
				}
			} else {
				pool.returnBrokenResource(jedis);
				return;
			}
		}
		// else close the connection normally (doing the try/catch dance)
		Exception exc = null;
		if (isQueueing()) {
			try {
				client.quit();
			} catch (Exception ex) {
				exc = ex;
			}
			try {
				client.disconnect();
			} catch (Exception ex) {
				exc = ex;
			}
			return;
		}
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getNativeConnection()
	 */
	@Override
	public Jedis getNativeConnection() {
		return jedis;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isClosed()
	 */
	@Override
	public boolean isClosed() {
		try {
			return !jedis.isConnected();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isQueueing()
	 */
	@Override
	public boolean isQueueing() {
		return client.isInMulti();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isPipelined()
	 */
	@Override
	public boolean isPipelined() {
		return (pipeline != null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#openPipeline()
	 */
	@Override
	public void openPipeline() {
		if (pipeline == null) {
			pipeline = jedis.pipelined();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#closePipeline()
	 */
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
		pipeline.sync();
		Exception cause = null;
		for (FutureResult<Response<?>> result : pipelinedResults) {
			try {
				Object data = result.get();
				if (!convertPipelineAndTxResults || !(result.isStatus())) {
					results.add(data);
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

	void pipeline(FutureResult<Response<?>> result) {
		if (isQueueing()) {
			transaction(result);
		} else {
			pipelinedResults.add(result);
		}
	}

	void transaction(FutureResult<Response<?>> result) {
		txResults.add(result);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#echo(byte[])
	 */
	@Override
	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.echo(message)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.echo(message)));
				return null;
			}
			return jedis.echo(message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#ping()
	 */
	@Override
	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.ping()));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.ping()));
				return null;
			}
			return jedis.ping();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#discard()
	 */
	@Override
	public void discard() {
		try {
			if (isPipelined()) {
				pipeline(new JedisStatusResult(pipeline.discard()));
				return;
			}
			transaction.discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#exec()
	 */
	@Override
	public List<Object> exec() {
		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.exec(),
						new TransactionResultConverter<>(new LinkedList<>(txResults), JedisConverters.exceptionConverter())));
				return null;
			}

			if (transaction == null) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction. Did you forget to call multi?");
			}
			List<Object> results = transaction.exec();
			return convertPipelineAndTxResults && !CollectionUtils.isEmpty(results)
					? new TransactionResultConverter<>(txResults, JedisConverters.exceptionConverter()).convert(results)
					: results;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	public Pipeline getPipeline() {
		return pipeline;
	}

	public Transaction getTransaction() {
		return transaction;
	}

	public Jedis getJedis() {
		return jedis;
	}

	JedisResult newJedisResult(Response<?> response) {
		return new JedisResult(response);
	}

	<T> JedisResult newJedisResult(Response<T> response, Converter<T, ?> converter) {
		return new JedisResult(response, converter);
	}

	JedisStatusResult newStatusResult(Response<?> response) {
		return new JedisStatusResult(response);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#multi()
	 */
	@Override
	public void multi() {
		if (isQueueing()) {
			return;
		}
		try {
			if (isPipelined()) {
				pipeline.multi();
				return;
			}
			this.transaction = jedis.multi();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
	 */
	@Override
	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(new JedisStatusResult(pipeline.select(dbIndex)));
				return;
			}
			if (isQueueing()) {
				transaction(new JedisStatusResult(transaction.select(dbIndex)));
				return;
			}
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#unwatch()
	 */
	@Override
	public void unwatch() {
		try {
			jedis.unwatch();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#watch(byte[][])
	 */
	@Override
	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			for (byte[] key : keys) {
				if (isPipelined()) {
					pipeline(new JedisStatusResult(pipeline.watch(key)));
				} else {
					jedis.watch(key);
				}
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#publish(byte[], byte[])
	 */
	@Override
	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.publish(channel, message)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.publish(channel, message)));
				return null;
			}
			return jedis.publish(channel, message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#getSubscription()
	 */
	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#isSubscribed()
	 */
	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#pSubscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
	 */
	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			jedis.psubscribe(jedisPubSub, patterns);

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#subscribe(org.springframework.data.redis.connection.MessageListener, byte[][])
	 */
	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			jedis.subscribe(jedisPubSub, channels);

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.AbstractRedisConnection#isActive(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	protected boolean isActive(RedisNode node) {

		if (node == null) {
			return false;
		}

		Jedis temp = null;
		try {
			temp = getJedis(node);
			temp.connect();
			return temp.ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (temp != null) {
				temp.disconnect();
				temp.close();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.AbstractRedisConnection#getSentinelConnection(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	protected JedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		return new JedisSentinelConnection(getJedis(sentinel));
	}

	protected Jedis getJedis(RedisNode node) {

		Jedis jedis = new Jedis(node.getHost(), node.getPort());

		if (StringUtils.hasText(clientName)) {
			jedis.clientSetname(clientName);
		}

		return jedis;
	}

}
