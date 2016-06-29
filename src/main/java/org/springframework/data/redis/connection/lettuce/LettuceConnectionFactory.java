/*
 * Copyright 2011-2016 the original author or authors.
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
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.resource.ClientResources;

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
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Balázs Németh
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
	private StatefulRedisConnection<byte[], byte[]> connection;
	private LettucePool pool;
	private int dbIndex = 0;
	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();
	private String password;
	private boolean convertPipelineAndTxResults = true;
	private RedisSentinelConfiguration sentinelConfiguration;
	private RedisClusterConfiguration clusterConfiguration;
	private ClusterCommandExecutor clusterCommandExecutor;
	private ClientResources clientResources;
	private boolean useSsl = false;
	private boolean verifyPeer = true;
	private boolean startTls = false;

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

		try {
			client.shutdown(shutdownTimeout, shutdownTimeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {

			if (log.isWarnEnabled()) {
				log.warn((client != null ? ClassUtils.getShortName(client.getClass()) : "LettuceClient")
						+ " did not shut down gracefully.", e);
			}
		}

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getReactiveConnection()
	 */
	@Override
	public LettuceReactiveRedisConnection getReactiveConnection() {
		return new LettuceReactiveRedisConnection(client);
	}

	@Override
	public LettuceReactiveRedisClusterConnection getReactiveClusterConnection() {
		if(!isClusterAware()) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured!");
		}

		return new LettuceReactiveRedisClusterConnection((RedisClusterClient)client);
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
					connection.sync().ping();
					valid = true;
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
	 * Sets to use SSL connection
	 *
	 * @param useSsl {@literal true} to use SSL.
	 */
	public void setUseSsl(boolean useSsl) {
		this.useSsl = useSsl;
	}

	/**
	 * Returns whether to use SSL.
	 *
	 * @return use of SSL
	 */
	public boolean isUseSsl() {
		return useSsl;
	}

	/**
	 * Sets to use verify certificate validity/hostname check when SSL is used.
	 *
	 * @param verifyPeer {@literal false} not to verify hostname.
	 */
	public void setVerifyPeer(boolean verifyPeer) {
		this.verifyPeer = verifyPeer;
	}

	/**
	 * Returns whether to verify certificate validity/hostname check when SSL is used.
	 *
	 * @return verify peers when using SSL
	 */
	public boolean isVerifyPeer() {
		return verifyPeer;
	}

	/**
	 * Returns whether to issue a StartTLS.
	 *
	 * @return use of StartTLS
	 */
	public boolean isStartTls() {
		return startTls;
	}

	/**
	 * Sets to issue StartTLS.
	 *
	 * @param startTls {@literal true} to issue StartTLS.
	 */
	public void setStartTls(boolean startTls) {
		this.startTls = startTls;
	}

	/**
	 * Indicates if validation of the native Lettuce connection is enabled.
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
	 * Get the {@link ClientResources} to reuse infrastructure.
	 *
	 * @return {@literal null} if not set.
	 * @since 1.7
	 */
	public ClientResources getClientResources() {
		return clientResources;
	}

	/**
	 * Sets the {@link ClientResources} to reuse the client infrastructure. <br />
	 * Set to {@literal null} to not share resources.
	 *
	 * @param clientResources can be {@literal null}.
	 * @since 1.7
	 */
	public void setClientResources(ClientResources clientResources) {
		this.clientResources = clientResources;
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

	protected StatefulRedisConnection<byte[], byte[]> getSharedConnection() {
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

	protected StatefulRedisConnection<byte[], byte[]> createLettuceConnector() {
		try {

			StatefulRedisConnection<byte[], byte[]> connection = null;
			if (client instanceof RedisClient) {
				connection = ((RedisClient) client).connect(LettuceConnection.CODEC);
				if (dbIndex > 0) {
					connection.sync().select(dbIndex);
				}
			} else {
				connection = null;
			}
			return connection;
		} catch (RedisException e) {
			throw new RedisConnectionFailureException("Unable to connect to Redis on " + getHostName() + ":" + getPort(), e);
		}
	}

	private AbstractRedisClient createRedisClient() {

		if (isRedisSentinelAware()) {

			RedisURI redisURI = getSentinelRedisURI();
			if (clientResources == null) {
				return RedisClient.create(redisURI);
			}

			return RedisClient.create(clientResources, redisURI);
		}

		if (isClusterAware()) {

			List<RedisURI> initialUris = new ArrayList<RedisURI>();
			for (RedisNode node : this.clusterConfiguration.getClusterNodes()) {
				initialUris.add(createRedisURIAndApplySettings(node.getHost(), node.getPort()));
			}

			RedisClusterClient clusterClient = clientResources != null
					? RedisClusterClient.create(clientResources, initialUris) : RedisClusterClient.create(initialUris);

			this.clusterCommandExecutor = new ClusterCommandExecutor(
					new LettuceClusterConnection.LettuceClusterTopologyProvider(clusterClient),
					new LettuceClusterConnection.LettuceClusterNodeResourceProvider(clusterClient), EXCEPTION_TRANSLATION);

			return clusterClient;
		}

		if (pool != null) {
			return pool.getClient();
		}

		RedisURI uri = createRedisURIAndApplySettings(hostName, port);
		return clientResources != null ? RedisClient.create(clientResources, uri) : RedisClient.create(uri);
	}

	private RedisURI getSentinelRedisURI() {

		RedisURI redisUri = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);

		if (StringUtils.hasText(password)) {
			redisUri.setPassword(password);
		}

		return redisUri;
	}

	private RedisURI createRedisURIAndApplySettings(String host, int port) {

		RedisURI.Builder builder = RedisURI.Builder.redis(host, port);
		if (StringUtils.hasText(password)) {
			builder.withPassword(password);
		}

		builder.withSsl(useSsl);
		builder.withVerifyPeer(verifyPeer);
		builder.withStartTls(startTls);
		builder.withTimeout(timeout, TimeUnit.MILLISECONDS);

		return builder.build();
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
		return new LettuceSentinelConnection(((RedisClient) client).connectSentinel());
	}
}
