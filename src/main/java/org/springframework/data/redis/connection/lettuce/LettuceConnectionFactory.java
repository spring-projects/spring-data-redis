/*
 * Copyright 2011-2015 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.util.Assert;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.LettuceFutures;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import org.springframework.util.StringUtils;

/**
 * Connection factory creating <a href="http://github.com/mp911de/lettuce">Lettuce</a>-based connections.
 * <p>
 * This factory creates a new {@link LettuceConnection} on each call to {@link #getConnection()}. Multiple
 * {@link LettuceConnection}s share a single thread-safe native connection by default.
 * <p>
 * The shared native connection is never closed by {@link LettuceConnection}, therefore it is not validated by default
 * on {@link #getConnection()}. Use {@link #setValidateConnection(boolean)} to change this behavior if necessary. Inject
 * a {@link Pool} to pool dedicated connections. If shareNativeConnection is true, the pool will be used to select a
 * connection for blocking and tx operations only, which should not share a connection. If native connection sharing is
 * disabled, the selected connection will be used for all operations.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class LettuceConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	public static final String PING_REPLY = "PONG";

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			LettuceConverters.exceptionConverter());

	private final Log log = LogFactory.getLog(getClass());

	private String hostName = "localhost";
	private int port = 6379;
	private AbstractRedisClient client;
	private long timeout = TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
	private long shutdownTimeout = TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS);
	private boolean validateConnection = false;
	private boolean shareNativeConnection = true;
	private RedisAsyncConnection<byte[], byte[]> connection;
	private LettucePool pool;
	private int dbIndex = 0;
	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();
	private String password;
	private boolean convertPipelineAndTxResults = true;
	private RedisSentinelConfiguration sentinelConfiguration;
	private RedisClusterConfiguration clusterConfiguration;
	private ClusterCommandExecutor clusterCommandExecutor;

	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance with default settings.
	 */
	public LettuceConnectionFactory() {}

	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance with default settings.
	 */
	public LettuceConnectionFactory(String host, int port) {
		this.hostName = host;
		this.port = port;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisSentinelConfiguration}
	 *
	 * @param sentinelConfiguration
	 * @since 1.6
	 */
	public LettuceConnectionFactory(RedisSentinelConfiguration sentinelConfiguration) {
		this.sentinelConfiguration = sentinelConfiguration;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisClusterConfiguration}
	 * applied to create a {@link RedisClusterClient}.
	 * 
	 * @param clusterConfig
	 * @since 1.7
	 */
	public LettuceConnectionFactory(RedisClusterConfiguration clusterConfig) {
		this.clusterConfiguration = clusterConfig;
	}

	public LettuceConnectionFactory(LettucePool pool) {
		this.pool = pool;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		this.client = createRedisClient();

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() {
		resetConnection();
		client.shutdown(shutdownTimeout, shutdownTimeout, TimeUnit.MILLISECONDS);

		if (clusterCommandExecutor != null) {

			try {
				clusterCommandExecutor.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close cluster command executor", ex);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getConnection()
	 */
	public RedisConnection getConnection() {

		if (isClusterAware()) {
			return getClusterConnection();
		}

		LettuceConnection connection = new LettuceConnection(getSharedConnection(), timeout, client, pool, dbIndex);
		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);
		return connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getClusterConnection()
	 */
	@Override
	public RedisClusterConnection getClusterConnection() {

		if (!isClusterAware()) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured!");
		}

		return new LettuceClusterConnection((RedisClusterClient) client, clusterCommandExecutor);
	}

	public void initConnection() {
		synchronized (this.connectionMonitor) {
			if (this.connection != null) {
				resetConnection();
			}
			this.connection = createLettuceConnector();
		}
	}

	/**
	 * Reset the underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if (this.connection != null) {
				this.connection.close();
			}
			this.connection = null;
		}
	}

	/**
	 * Validate the shared Connection and reinitialize if invalid
	 */
	public void validateConnection() {
		synchronized (this.connectionMonitor) {

			boolean valid = false;

			if (connection.isOpen()) {
				try {
					RedisFuture<String> ping = connection.ping();
					LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, ping);
					if (PING_REPLY.equalsIgnoreCase(ping.get())) {
						valid = true;
					}
				} catch (Exception e) {
					log.debug("Validation failed", e);
				}
			}

			if (!valid) {
				log.warn("Validation of shared connection failed. Creating a new connection.");
				initConnection();
			}
		}
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/**
	 * Returns the current host.
	 * 
	 * @return the host
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the host.
	 * 
	 * @param host the host to set
	 */
	public void setHostName(String host) {
		this.hostName = host;
	}

	/**
	 * Returns the current port.
	 * 
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the port.
	 * 
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Returns the connection timeout (in milliseconds).
	 * 
	 * @return connection timeout
	 */
	public long getTimeout() {
		return timeout;
	}

	/**
	 * Sets the connection timeout (in milliseconds).
	 * 
	 * @param timeout connection timeout
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	/**
	 * Indicates if validation of the native Lettuce connection is enabled
	 * 
	 * @return connection validation enabled
	 */
	public boolean getValidateConnection() {
		return validateConnection;
	}

	/**
	 * Enables validation of the shared native Lettuce connection on calls to {@link #getConnection()}. A new connection
	 * will be created and used if validation fails.
	 * <p>
	 * Lettuce will automatically reconnect until close is called, which should never happen through
	 * {@link LettuceConnection} if a shared native connection is used, therefore the default is false.
	 * <p>
	 * Setting this to true will result in a round-trip call to the server on each new connection, so this setting should
	 * only be used if connection sharing is enabled and there is code that is actively closing the native Lettuce
	 * connection.
	 * 
	 * @param validateConnection enable connection validation
	 */
	public void setValidateConnection(boolean validateConnection) {
		this.validateConnection = validateConnection;
	}

	/**
	 * Indicates if multiple {@link LettuceConnection}s should share a single native connection.
	 * 
	 * @return native connection shared
	 */
	public boolean getShareNativeConnection() {
		return shareNativeConnection;
	}

	/**
	 * Enables multiple {@link LettuceConnection}s to share a single native connection. If set to false, every operation
	 * on {@link LettuceConnection} will open and close a socket.
	 * 
	 * @param shareNativeConnection enable connection sharing
	 */
	public void setShareNativeConnection(boolean shareNativeConnection) {
		this.shareNativeConnection = shareNativeConnection;
	}

	/**
	 * Returns the index of the database.
	 * 
	 * @return Returns the database index
	 */
	public int getDatabase() {
		return dbIndex;
	}

	/**
	 * Sets the index of the database used by this connection factory. Default is 0.
	 * 
	 * @param index database index
	 */
	public void setDatabase(int index) {
		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");
		this.dbIndex = index;
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 * 
	 * @return password for authentication
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 * 
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Returns the shutdown timeout for shutting down the RedisClient (in milliseconds).
	 * 
	 * @return shutdown timeout
	 * @since 1.6
	 */
	public long getShutdownTimeout() {
		return shutdownTimeout;
	}

	/**
	 * Sets the shutdown timeout for shutting down the RedisClient (in milliseconds).
	 * 
	 * @param shutdownTimeout the shutdown timeout
	 * @since 1.6
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link LettuceConnection#closePipeline()} and {LettuceConnection#exec()} will be of the type returned by the
	 * Lettuce driver
	 * 
	 * @return Whether or not to convert pipeline and tx results
	 */
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined and transaction results should be converted to the expected data type. If false, results of
	 * {@link LettuceConnection#closePipeline()} and {LettuceConnection#exec()} will be of the type returned by the
	 * Lettuce driver
	 * 
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	protected RedisAsyncConnection<byte[], byte[]> getSharedConnection() {
		if (shareNativeConnection) {
			synchronized (this.connectionMonitor) {
				if (this.connection == null) {
					initConnection();
				}
				if (validateConnection) {
					validateConnection();
				}
				return this.connection;
			}
		} else {
			return null;
		}
	}

	protected RedisAsyncConnection<byte[], byte[]> createLettuceConnector() {
		try {

			RedisAsyncConnection connection = null;
			if (client instanceof RedisClient) {
				connection = ((RedisClient) client).connectAsync(LettuceConnection.CODEC);
				if (dbIndex > 0) {
					connection.select(dbIndex);
				}
			} else {
				connection = (RedisAsyncConnection<byte[], byte[]>) ((RedisClusterClient) client)
						.connectClusterAsync(LettuceConnection.CODEC);
			}
			return connection;
		} catch (RedisException e) {
			throw new RedisConnectionFailureException("Unable to connect to Redis on " + getHostName() + ":" + getPort(), e);
		}
	}

	private AbstractRedisClient createRedisClient() {

		if (isRedisSentinelAware()) {
			RedisURI redisURI = getSentinelRedisURI();
			return new RedisClient(redisURI);
		}

		if (isClusterAware()) {

			List<RedisURI> initialUris = new ArrayList<RedisURI>();
			for (RedisNode node : this.clusterConfiguration.getClusterNodes()) {
				long timeout = this.clusterConfiguration.getClusterTimeout() != null ? this.clusterConfiguration.getClusterTimeout() : this.timeout;
				RedisURI redisURI = new RedisURI(node.getHost(), node.getPort(), timeout, TimeUnit.MILLISECONDS);

				if(StringUtils.hasText(this.clusterConfiguration.getPassword())){
					redisURI.setPassword(this.clusterConfiguration.getPassword());
				}else  if(StringUtils.hasText(password)){
					redisURI.setPassword(password);
				}

				initialUris.add(redisURI);
			}

			RedisClusterClient clusterClient = new RedisClusterClient(initialUris);

			this.clusterCommandExecutor = new ClusterCommandExecutor(
					new LettuceClusterConnection.LettuceClusterTopologyProvider(clusterClient),
					new LettuceClusterConnection.LettuceClusterNodeResourceProvider(clusterClient), EXCEPTION_TRANSLATION);

			return clusterClient;
		}

		if (pool != null) {
			return pool.getClient();
		}

		RedisURI.Builder builder = RedisURI.Builder.redis(hostName, port);
		if (password != null) {
			builder.withPassword(password);
		}
		builder.withTimeout(timeout, TimeUnit.MILLISECONDS);
		RedisClient client = new RedisClient(builder.build());
		return client;
	}

	private RedisURI getSentinelRedisURI() {
		return LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);
	}

	/**
	 * @return true when {@link RedisSentinelConfiguration} is present.
	 * @since 1.5
	 */
	public boolean isRedisSentinelAware() {
		return sentinelConfiguration != null;
	}

	/**
	 * @return since 1.7
	 */
	public boolean isClusterAware() {
		return clusterConfiguration != null;
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		if (!(client instanceof RedisClient)) {
			throw new InvalidDataAccessResourceUsageException("Unable to connect to sentinels using " + client.getClass());
		}
		return new LettuceSentinelConnection(((RedisClient) client).connectSentinelAsync());
	}
}
