/*
 * Copyright 2011-2018 the original author or authors.
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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.RedisConfiguration.ClusterConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration.DomainSocketConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex;
import org.springframework.data.redis.connection.RedisConfiguration.WithPassword;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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
 * <p>
 * {@link LettuceConnectionFactory} should be configured using an environmental configuration and the
 * {@link LettuceConnectionFactory client configuration}. Lettuce supports the following environmental configurations:
 * <ul>
 * <li>{@link RedisStandaloneConfiguration}</li>
 * <li>{@link RedisStaticMasterSlaveConfiguration}</li>
 * <li>{@link RedisSocketConfiguration}</li>
 * <li>{@link RedisSentinelConfiguration}</li>
 * <li>{@link RedisClusterConfiguration}</li>
 * </ul>
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Balázs Németh
 * @author Ruben Cervilla
 */
public class LettuceConnectionFactory
		implements InitializingBean, DisposableBean, RedisConnectionFactory, ReactiveRedisConnectionFactory {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			LettuceConverters.exceptionConverter());

	private final Log log = LogFactory.getLog(getClass());
	private final LettuceClientConfiguration clientConfiguration;

	private @Nullable AbstractRedisClient client;
	private @Nullable LettuceConnectionProvider connectionProvider;
	private @Nullable LettuceConnectionProvider reactiveConnectionProvider;
	private boolean validateConnection = false;
	private boolean shareNativeConnection = true;
	private @Nullable SharedConnection<byte[]> connection;
	private @Nullable SharedConnection<ByteBuffer> reactiveConnection;
	private @Nullable LettucePool pool;
	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();
	private boolean convertPipelineAndTxResults = true;

	private RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration("localhost", 6379);

	private @Nullable RedisConfiguration configuration;

	private @Nullable ClusterCommandExecutor clusterCommandExecutor;

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance with default settings.
	 */
	public LettuceConnectionFactory() {
		this(new MutableLettuceClientConfiguration());
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance with default settings.
	 */
	public LettuceConnectionFactory(RedisStandaloneConfiguration configuration) {
		this(configuration, new MutableLettuceClientConfiguration());
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance given {@link LettuceClientConfiguration}.
	 *
	 * @param clientConfig must not be {@literal null}
	 * @since 2.0
	 */
	private LettuceConnectionFactory(LettuceClientConfiguration clientConfig) {

		Assert.notNull(clientConfig, "LettuceClientConfiguration must not be null!");

		this.clientConfiguration = clientConfig;
		this.configuration = this.standaloneConfig;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance with default settings.
	 */
	public LettuceConnectionFactory(String host, int port) {
		this(new RedisStandaloneConfiguration(host, port), new MutableLettuceClientConfiguration());
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisSocketConfiguration}.
	 *
	 * @param redisConfiguration must not be {@literal null}.
	 * @since 2.1
	 */
	public LettuceConnectionFactory(RedisConfiguration redisConfiguration) {
		this(redisConfiguration, new MutableLettuceClientConfiguration());
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisSentinelConfiguration}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 * @since 1.6
	 */
	public LettuceConnectionFactory(RedisSentinelConfiguration sentinelConfiguration) {
		this(sentinelConfiguration, new MutableLettuceClientConfiguration());
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisClusterConfiguration}
	 * applied to create a {@link RedisClusterClient}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @since 1.7
	 */
	public LettuceConnectionFactory(RedisClusterConfiguration clusterConfiguration) {
		this(clusterConfiguration, new MutableLettuceClientConfiguration());
	}

	/**
	 * @param pool
	 * @deprecated since 2.0, use pooling via {@link LettucePoolingClientConfiguration}.
	 */
	@Deprecated
	public LettuceConnectionFactory(LettucePool pool) {

		this(new MutableLettuceClientConfiguration());
		this.pool = pool;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisStandaloneConfiguration} and
	 * {@link LettuceClientConfiguration}.
	 *
	 * @param standaloneConfig must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public LettuceConnectionFactory(RedisStandaloneConfiguration standaloneConfig,
			LettuceClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(standaloneConfig, "RedisStandaloneConfiguration must not be null!");

		this.standaloneConfig = standaloneConfig;
		this.configuration = this.standaloneConfig;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given
	 * {@link RedisStaticMasterSlaveConfiguration} and {@link LettuceClientConfiguration}.
	 *
	 * @param redisConfiguration must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.1
	 */
	public LettuceConnectionFactory(RedisConfiguration redisConfiguration, LettuceClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(redisConfiguration, "RedisConfiguration must not be null!");

		this.configuration = redisConfiguration;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisSentinelConfiguration} and
	 * {@link LettuceClientConfiguration}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public LettuceConnectionFactory(RedisSentinelConfiguration sentinelConfiguration,
			LettuceClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(sentinelConfiguration, "RedisSentinelConfiguration must not be null!");

		this.configuration = sentinelConfiguration;
	}

	/**
	 * Constructs a new {@link LettuceConnectionFactory} instance using the given {@link RedisClusterConfiguration} and
	 * {@link LettuceClientConfiguration}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public LettuceConnectionFactory(RedisClusterConfiguration clusterConfiguration,
			LettuceClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(clusterConfiguration, "RedisClusterConfiguration must not be null!");

		this.configuration = clusterConfiguration;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {

		this.client = createClient();

		this.connectionProvider = createConnectionProvider(client, LettuceConnection.CODEC);
		this.reactiveConnectionProvider = createConnectionProvider(client, LettuceReactiveRedisConnection.CODEC);

		if (isClusterAware()) {
			this.clusterCommandExecutor = new ClusterCommandExecutor(
					new LettuceClusterTopologyProvider((RedisClusterClient) client),
					new LettuceClusterConnection.LettuceClusterNodeResourceProvider(this.connectionProvider),
					EXCEPTION_TRANSLATION);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() {

		resetConnection();

		if (connectionProvider instanceof DisposableBean) {
			try {
				((DisposableBean) connectionProvider).destroy();
			} catch (Exception e) {

				if (log.isWarnEnabled()) {
					log.warn(connectionProvider + " did not shut down gracefully.", e);
				}
			}
		}

		try {
			Duration timeout = clientConfiguration.getShutdownTimeout();
			client.shutdown(timeout.toMillis(), timeout.toMillis(), TimeUnit.MILLISECONDS);
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

		LettuceConnection connection;

		if (pool != null) {
			connection = new LettuceConnection(getSharedConnection(), getTimeout(), null, pool, getDatabase());
		} else {
			connection = new LettuceConnection(getSharedConnection(), connectionProvider, getTimeout(), getDatabase());
		}

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

		RedisClusterClient clusterClient = (RedisClusterClient) client;

		return getShareNativeConnection()
				? new LettuceClusterConnection(
						(StatefulRedisClusterConnection<byte[], byte[]>) getOrCreateSharedConnection().getConnection(),
						connectionProvider, clusterClient, clusterCommandExecutor, clientConfiguration.getCommandTimeout())
				: new LettuceClusterConnection(null, connectionProvider, clusterClient, clusterCommandExecutor,
						clientConfiguration.getCommandTimeout());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnectionFactory#getReactiveConnection()
	 */
	@Override
	public LettuceReactiveRedisConnection getReactiveConnection() {

		return getShareNativeConnection()
				? new LettuceReactiveRedisConnection(getSharedReactiveConnection(), reactiveConnectionProvider)
				: new LettuceReactiveRedisConnection(reactiveConnectionProvider);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnectionFactory#getReactiveClusterConnection()
	 */
	@Override
	public LettuceReactiveRedisClusterConnection getReactiveClusterConnection() {

		if (!isClusterAware()) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured!");
		}

		RedisClusterClient client = (RedisClusterClient) this.client;

		return getShareNativeConnection()
				? new LettuceReactiveRedisClusterConnection(getSharedReactiveConnection(), reactiveConnectionProvider, client)
				: new LettuceReactiveRedisClusterConnection(reactiveConnectionProvider, client);
	}

	/**
	 * Initialize the shared connection if {@link #getShareNativeConnection() native connection sharing} is enabled and
	 * reset any previously existing connection.
	 */
	public void initConnection() {

		resetConnection();

		getSharedConnection();
		getSharedReactiveConnection();
	}

	/**
	 * Reset the underlying shared Connection, to be reinitialized on next access.
	 */
	public void resetConnection() {

		Optionals.toStream(Optional.ofNullable(connection), Optional.ofNullable(reactiveConnection))
				.forEach(SharedConnection::resetConnection);

		synchronized (this.connectionMonitor) {

			this.connection = null;
			this.reactiveConnection = null;
		}
	}

	/**
	 * Validate the shared connections and reinitialize if invalid.
	 */
	public void validateConnection() {

		getOrCreateSharedConnection().validateConnection();
		getOrCreateSharedReactiveConnection().validateConnection();
	}

	private SharedConnection<byte[]> getOrCreateSharedConnection() {

		synchronized (this.connectionMonitor) {

			if (this.connection == null) {
				this.connection = new SharedConnection<>(connectionProvider, false);
			}

			return this.connection;
		}
	}

	private SharedConnection<ByteBuffer> getOrCreateSharedReactiveConnection() {

		synchronized (this.connectionMonitor) {

			if (this.reactiveConnection == null) {
				this.reactiveConnection = new SharedConnection<>(reactiveConnectionProvider, true);
			}

			return this.reactiveConnection;
		}
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/**
	 * Returns the current host.
	 *
	 * @return the host.
	 */
	public String getHostName() {
		return standaloneConfig.getHostName();
	}

	/**
	 * Sets the hostname.
	 *
	 * @param hostName the hostname to set.
	 * @deprecated since 2.0, configure the hostname using {@link RedisStandaloneConfiguration}.
	 */
	@Deprecated
	public void setHostName(String hostName) {
		standaloneConfig.setHostName(hostName);
	}

	/**
	 * Returns the current port.
	 *
	 * @return the port.
	 */
	public int getPort() {
		return standaloneConfig.getPort();
	}

	/**
	 * Sets the port.
	 *
	 * @param port the port to set.
	 * @deprecated since 2.0, configure the port using {@link RedisStandaloneConfiguration}.
	 */
	@Deprecated
	public void setPort(int port) {
		standaloneConfig.setPort(port);
	}

	/**
	 * Returns the connection timeout (in milliseconds).
	 *
	 * @return connection timeout.
	 */
	public long getTimeout() {
		return getClientTimeout();
	}

	/**
	 * Sets the connection timeout (in milliseconds).
	 *
	 * @param timeout the timeout.
	 * @deprecated since 2.0, configure the timeout using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setTimeout(long timeout) {
		getMutableConfiguration().setTimeout(Duration.ofMillis(timeout));
	}

	/**
	 * Returns whether to use SSL.
	 *
	 * @return use of SSL.
	 */
	public boolean isUseSsl() {
		return clientConfiguration.isUseSsl();
	}

	/**
	 * Sets to use SSL connection.
	 *
	 * @param useSsl {@literal true} to use SSL.
	 * @deprecated since 2.0, configure SSL usage using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setUseSsl(boolean useSsl) {
		getMutableConfiguration().setUseSsl(useSsl);
	}

	/**
	 * Returns whether to verify certificate validity/hostname check when SSL is used.
	 *
	 * @return whether to verify peers when using SSL.
	 */
	public boolean isVerifyPeer() {
		return clientConfiguration.isVerifyPeer();
	}

	/**
	 * Sets to use verify certificate validity/hostname check when SSL is used.
	 *
	 * @param verifyPeer {@literal false} not to verify hostname.
	 * @deprecated since 2.0, configure peer verification using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setVerifyPeer(boolean verifyPeer) {
		getMutableConfiguration().setVerifyPeer(verifyPeer);
	}

	/**
	 * Returns whether to issue a StartTLS.
	 *
	 * @return use of StartTLS.
	 */
	public boolean isStartTls() {
		return clientConfiguration.isStartTls();
	}

	/**
	 * Sets to issue StartTLS.
	 *
	 * @param startTls {@literal true} to issue StartTLS.
	 * @deprecated since 2.0, configure StartTLS using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setStartTls(boolean startTls) {
		getMutableConfiguration().setStartTls(startTls);
	}

	/**
	 * Indicates if validation of the native Lettuce connection is enabled.
	 *
	 * @return connection validation enabled.
	 */
	public boolean getValidateConnection() {
		return validateConnection;
	}

	/**
	 * Enables validation of the shared native Lettuce connection on calls to {@link #getConnection()}. A new connection
	 * will be created and used if validation fails.
	 * <p>
	 * Lettuce will automatically reconnect until close is called, which should never happen through
	 * {@link LettuceConnection} if a shared native connection is used, therefore the default is {@literal false}.
	 * <p>
	 * Setting this to {@literal true} will result in a round-trip call to the server on each new connection, so this
	 * setting should only be used if connection sharing is enabled and there is code that is actively closing the native
	 * Lettuce connection.
	 *
	 * @param validateConnection enable connection validation.
	 */
	public void setValidateConnection(boolean validateConnection) {
		this.validateConnection = validateConnection;
	}

	/**
	 * Indicates if multiple {@link LettuceConnection}s should share a single native connection.
	 *
	 * @return native connection shared.
	 */
	public boolean getShareNativeConnection() {
		return shareNativeConnection;
	}

	/**
	 * Enables multiple {@link LettuceConnection}s to share a single native connection. If set to {@literal false}, every
	 * operation on {@link LettuceConnection} will open and close a socket.
	 *
	 * @param shareNativeConnection enable connection sharing.
	 */
	public void setShareNativeConnection(boolean shareNativeConnection) {
		this.shareNativeConnection = shareNativeConnection;
	}

	/**
	 * Returns the index of the database.
	 *
	 * @return the database index.
	 */
	public int getDatabase() {
		return RedisConfiguration.getDatabaseOrElse(configuration, standaloneConfig::getDatabase);
	}

	/**
	 * Sets the index of the database used by this connection factory. Default is 0.
	 *
	 * @param index database index
	 */
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");

		if (RedisConfiguration.isDatabaseIndexAware(configuration)) {

			((WithDatabaseIndex) configuration).setDatabase(index);
			return;
		}

		standaloneConfig.setDatabase(index);
	}

	/**
	 * Returns the client name.
	 *
	 * @return the client name or {@literal null} if not set.
	 * @since 2.1
	 */
	@Nullable
	public String getClientName() {
		return clientConfiguration.getClientName().orElse(null);
	}

	/**
	 * Sets the client name used by this connection factory.
	 *
	 * @param clientName the client name. Can be {@literal null}.
	 * @since 2.1
	 * @deprecated configure the client name using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setClientName(@Nullable String clientName) {
		this.getMutableConfiguration().setClientName(clientName);
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 *
	 * @return password for authentication or {@literal null} if not set.
	 */
	@Nullable
	public String getPassword() {
		return getRedisPassword().map(String::new).orElse(null);
	}

	private RedisPassword getRedisPassword() {
		return RedisConfiguration.getPasswordOrElse(configuration, standaloneConfig::getPassword);
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 *
	 * @param password the password to set
	 * @deprecated since 2.0, configure the password using {@link RedisStandaloneConfiguration},
	 *             {@link RedisSentinelConfiguration} or {@link RedisClusterConfiguration}.
	 */
	@Deprecated
	public void setPassword(String password) {

		if (RedisConfiguration.isPasswordAware(configuration)) {

			((WithPassword) configuration).setPassword(password);
			return;
		}

		standaloneConfig.setPassword(RedisPassword.of(password));
	}

	/**
	 * Returns the shutdown timeout for shutting down the RedisClient (in milliseconds).
	 *
	 * @return shutdown timeout.
	 * @since 1.6
	 */
	public long getShutdownTimeout() {
		return clientConfiguration.getShutdownTimeout().toMillis();
	}

	/**
	 * Sets the shutdown timeout for shutting down the RedisClient (in milliseconds).
	 *
	 * @param shutdownTimeout the shutdown timeout.
	 * @since 1.6
	 * @deprecated since 2.0, configure the shutdown timeout using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setShutdownTimeout(long shutdownTimeout) {
		getMutableConfiguration().setShutdownTimeout(Duration.ofMillis(shutdownTimeout));
	}

	/**
	 * Get the {@link ClientResources} to reuse infrastructure.
	 *
	 * @return {@literal null} if not set.
	 * @since 1.7
	 */
	public ClientResources getClientResources() {
		return clientConfiguration.getClientResources().orElse(null);
	}

	/**
	 * Sets the {@link ClientResources} to reuse the client infrastructure. <br />
	 * Set to {@literal null} to not share resources.
	 *
	 * @param clientResources can be {@literal null}.
	 * @since 1.7
	 * @deprecated since 2.0, configure {@link ClientResources} using {@link LettuceClientConfiguration}.
	 * @throws IllegalStateException if {@link LettuceClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setClientResources(ClientResources clientResources) {
		getMutableConfiguration().setClientResources(clientResources);
	}

	/**
	 * @return the {@link LettuceClientConfiguration}.
	 * @since 2.0
	 */
	public LettuceClientConfiguration getClientConfiguration() {
		return clientConfiguration;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}.
	 * @since 2.0
	 */
	public RedisStandaloneConfiguration getStandaloneConfiguration() {
		return standaloneConfig;
	}

	/**
	 * @return the {@link RedisSocketConfiguration} or {@literal null} if not set.
	 * @since 2.1
	 */
	@Nullable
	public RedisSocketConfiguration getSocketConfiguration() {
		return isDomainSocketAware() ? (RedisSocketConfiguration) configuration : null;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}, may be {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	public RedisSentinelConfiguration getSentinelConfiguration() {
		return isRedisSentinelAware() ? (RedisSentinelConfiguration) configuration : null;
	}

	/**
	 * @return the {@link RedisClusterConfiguration}, may be {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	public RedisClusterConfiguration getClusterConfiguration() {
		return isClusterAware() ? (RedisClusterConfiguration) configuration : null;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link LettuceConnection#closePipeline()} and {LettuceConnection#exec()} will be of the type returned by the
	 * Lettuce driver.
	 *
	 * @return Whether or not to convert pipeline and tx results.
	 */
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined and transaction results should be converted to the expected data type. If false, results of
	 * {@link LettuceConnection#closePipeline()} and {LettuceConnection#exec()} will be of the type returned by the
	 * Lettuce driver.
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/**
	 * @return true when {@link RedisStaticMasterSlaveConfiguration} is present.
	 * @since 2.1
	 */
	private boolean isStaticMasterSlaveAware() {
		return RedisConfiguration.isStaticMasterSlaveConfiguration(configuration);
	}

	/**
	 * @return true when {@link RedisSentinelConfiguration} is present.
	 * @since 1.5
	 */
	public boolean isRedisSentinelAware() {
		return RedisConfiguration.isSentinelConfiguration(configuration);
	}

	/**
	 * @return true when {@link RedisSocketConfiguration} is present.
	 * @since 2.1
	 */
	private boolean isDomainSocketAware() {
		return RedisConfiguration.isDomainSocketConfiguration(configuration);
	}

	/**
	 * @return true when {@link RedisClusterConfiguration} is present.
	 * @since 1.7
	 */
	public boolean isClusterAware() {
		return RedisConfiguration.isClusterConfiguration(configuration);
	}

	/**
	 * @return the shared connection using {@literal byte} array encoding for imperative API use. {@literal null} if
	 *         {@link #getShareNativeConnection() connection sharing} is disabled.
	 */
	@Nullable
	protected StatefulRedisConnection<byte[], byte[]> getSharedConnection() {
		return shareNativeConnection ? (StatefulRedisConnection) getOrCreateSharedConnection().getConnection() : null;
	}

	/**
	 * @return the shared connection using {@link ByteBuffer} encoding for reactive API use. {@literal null} if
	 *         {@link #getShareNativeConnection() connection sharing} is disabled.
	 * @since 2.0.1
	 */
	@Nullable
	protected StatefulConnection<ByteBuffer, ByteBuffer> getSharedReactiveConnection() {
		return shareNativeConnection ? getOrCreateSharedReactiveConnection().getConnection() : null;
	}

	private LettuceConnectionProvider createConnectionProvider(AbstractRedisClient client, RedisCodec<?, ?> codec) {

		LettuceConnectionProvider connectionProvider = doCreateConnectionProvider(client, codec);

		if (this.clientConfiguration instanceof LettucePoolingClientConfiguration) {
			return new LettucePoolingConnectionProvider(connectionProvider,
					(LettucePoolingClientConfiguration) this.clientConfiguration);
		}

		return connectionProvider;
	}

	/**
	 * Create a {@link LettuceConnectionProvider} given {@link AbstractRedisClient} and {@link RedisCodec}. Configuration
	 * of this connection factory specifies the type of the created connection provider. This method creates either a
	 * {@link LettuceConnectionProvider} for either {@link RedisClient} or {@link RedisClusterClient}. Subclasses may
	 * override this method to decorate the connection provider.
	 *
	 * @param client either {@link RedisClient} or {@link RedisClusterClient}, must not be {@literal null}.
	 * @param codec used for connection creation, must not be {@literal null}. By default, a {@code byte[]} codec.
	 *          Reactive connections require a {@link java.nio.ByteBuffer} codec.
	 * @return the connection provider.
	 * @since 2.1
	 */
	protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client, RedisCodec<?, ?> codec) {

		ReadFrom readFrom = getClientConfiguration().getReadFrom().orElse(null);

		if (isStaticMasterSlaveAware()) {

			List<RedisURI> nodes = ((RedisStaticMasterSlaveConfiguration) configuration).getNodes().stream() //
					.map(it -> createRedisURIAndApplySettings(it.getHostName(), it.getPort())) //
					.peek(it -> it.setDatabase(getDatabase())) //
					.collect(Collectors.toList());

			return new StaticMasterSlaveConnectionProvider((RedisClient) client, codec, nodes, readFrom);
		}

		if (isClusterAware()) {
			return new ClusterConnectionProvider((RedisClusterClient) client, codec, readFrom);
		}

		return new StandaloneConnectionProvider((RedisClient) client, codec, readFrom);
	}

	protected AbstractRedisClient createClient() {

		if (isStaticMasterSlaveAware()) {

			RedisClient redisClient = clientConfiguration.getClientResources() //
					.map(RedisClient::create) //
					.orElseGet(RedisClient::create);

			clientConfiguration.getClientOptions().ifPresent(redisClient::setOptions);

			return redisClient;
		}

		if (isRedisSentinelAware()) {

			RedisURI redisURI = getSentinelRedisURI();
			RedisClient redisClient = clientConfiguration.getClientResources() //
					.map(clientResources -> RedisClient.create(clientResources, redisURI)) //
					.orElseGet(() -> RedisClient.create(redisURI));

			clientConfiguration.getClientOptions().ifPresent(redisClient::setOptions);
			return redisClient;
		}

		if (isClusterAware()) {

			List<RedisURI> initialUris = new ArrayList<>();
			for (RedisNode node : ((ClusterConfiguration) configuration).getClusterNodes()) {
				initialUris.add(createRedisURIAndApplySettings(node.getHost(), node.getPort()));
			}

			RedisClusterClient clusterClient = clientConfiguration.getClientResources() //
					.map(clientResources -> RedisClusterClient.create(clientResources, initialUris)) //
					.orElseGet(() -> RedisClusterClient.create(initialUris));

			clientConfiguration.getClientOptions() //
					.filter(clientOptions -> clientOptions instanceof ClusterClientOptions) //
					.ifPresent(clientOptions -> clusterClient.setOptions((ClusterClientOptions) clientOptions));

			return clusterClient;
		}

		RedisURI uri = isDomainSocketAware()
				? createRedisSocketURIAndApplySettings(((DomainSocketConfiguration) configuration).getSocket())
				: createRedisURIAndApplySettings(getHostName(), getPort());

		RedisClient redisClient = clientConfiguration.getClientResources() //
				.map(clientResources -> RedisClient.create(clientResources, uri)) //
				.orElseGet(() -> RedisClient.create(uri));
		clientConfiguration.getClientOptions().ifPresent(redisClient::setOptions);

		return redisClient;
	}

	private RedisURI getSentinelRedisURI() {

		RedisURI redisUri = LettuceConverters
				.sentinelConfigurationToRedisURI((org.springframework.data.redis.connection.RedisSentinelConfiguration) configuration);

		getRedisPassword().toOptional().ifPresent(redisUri::setPassword);
		clientConfiguration.getClientName().ifPresent(redisUri::setClientName);
		redisUri.setTimeout(clientConfiguration.getCommandTimeout());
		redisUri.setDatabase(getDatabase());

		return redisUri;
	}

	private RedisURI createRedisURIAndApplySettings(String host, int port) {

		RedisURI.Builder builder = RedisURI.Builder.redis(host, port);

		getRedisPassword().toOptional().ifPresent(builder::withPassword);
		clientConfiguration.getClientName().ifPresent(builder::withClientName);

		builder.withSsl(clientConfiguration.isUseSsl());
		builder.withVerifyPeer(clientConfiguration.isVerifyPeer());
		builder.withStartTls(clientConfiguration.isStartTls());
		builder.withTimeout(clientConfiguration.getCommandTimeout());

		return builder.build();
	}

	private RedisURI createRedisSocketURIAndApplySettings(String socketPath) {

		RedisURI.Builder builder = RedisURI.Builder.socket(socketPath);

		getRedisPassword().toOptional().ifPresent(builder::withPassword);
		builder.withTimeout(clientConfiguration.getCommandTimeout());

		return builder.build();
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		return new LettuceSentinelConnection(connectionProvider);
	}

	private MutableLettuceClientConfiguration getMutableConfiguration() {

		Assert.state(clientConfiguration instanceof MutableLettuceClientConfiguration,
				() -> String.format("Client configuration must be instance of MutableLettuceClientConfiguration but is %s",
						ClassUtils.getShortName(clientConfiguration.getClass())));

		return (MutableLettuceClientConfiguration) clientConfiguration;
	}

	private long getClientTimeout() {
		return clientConfiguration.getCommandTimeout().toMillis();
	}

	/**
	 * Wrapper for shared connections. Keeps track of the connection lifecycleThe wrapper is thread-safe as it
	 * synchronizes concurrent calls by blocking.
	 *
	 * @param <E> connection encoding.
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@RequiredArgsConstructor
	class SharedConnection<E> {

		private final LettuceConnectionProvider connectionProvider;
		private final boolean shareNativeClusterConnection;

		/** Synchronization monitor for the shared Connection */
		private final Object connectionMonitor = new Object();

		private @Nullable StatefulConnection<E, E> connection;

		/**
		 * Returns a valid Lettuce connection. Initializes and validates the connection if
		 * {@link #setValidateConnection(boolean) enabled}.
		 *
		 * @return the connection.
		 */
		@Nullable
		StatefulConnection<E, E> getConnection() {

			synchronized (this.connectionMonitor) {

				if (this.connection == null) {
					this.connection = getNativeConnection();
				}

				if (getValidateConnection()) {
					validateConnection();
				}

				return this.connection;
			}
		}

		/**
		 * Obtain a connection from the associated {@link LettuceConnectionProvider}.
		 *
		 * @return the connection.
		 */
		private StatefulConnection<E, E> getNativeConnection() {

			try {

				StatefulConnection<E, E> connection = connectionProvider.getConnection(StatefulConnection.class);
				if (connection instanceof StatefulRedisConnection && getDatabase() > 0) {
					((StatefulRedisConnection) connection).sync().select(getDatabase());
				}

				return connection;
			} catch (RedisException e) {
				throw new RedisConnectionFailureException("Unable to connect to Redis", e);
			}
		}

		/**
		 * Validate the connection. Invalid connections will be closed and the connection state will be reset.
		 */
		void validateConnection() {

			synchronized (this.connectionMonitor) {

				boolean valid = false;

				if (connection != null && connection.isOpen()) {
					try {

						if (connection instanceof StatefulRedisConnection) {
							((StatefulRedisConnection) connection).sync().ping();
						}

						if (connection instanceof StatefulRedisClusterConnection) {
							((StatefulRedisConnection) connection).sync().ping();
						}
						valid = true;
					} catch (Exception e) {
						log.debug("Validation failed", e);
					}
				}

				if (!valid) {

					if (connection != null) {
						connectionProvider.release(connection);
					}

					log.warn("Validation of shared connection failed. Creating a new connection.");

					resetConnection();
					this.connection = getNativeConnection();
				}
			}
		}

		/**
		 * Reset the underlying shared Connection, to be reinitialized on next access.
		 */
		void resetConnection() {

			synchronized (this.connectionMonitor) {

				if (this.connection != null) {
					this.connectionProvider.release(this.connection);
				}

				this.connection = null;
			}
		}
	}

	/**
	 * Mutable implementation of {@link LettuceClientConfiguration}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	static class MutableLettuceClientConfiguration implements LettuceClientConfiguration {

		private boolean useSsl;
		private boolean verifyPeer = true;
		private boolean startTls;
		private @Nullable ClientResources clientResources;
		private @Nullable String clientName;
		private Duration timeout = Duration.ofSeconds(RedisURI.DEFAULT_TIMEOUT);
		private Duration shutdownTimeout = Duration.ofMillis(100);

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isUseSsl()
		 */
		@Override
		public boolean isUseSsl() {
			return useSsl;
		}

		void setUseSsl(boolean useSsl) {
			this.useSsl = useSsl;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isVerifyPeer()
		 */
		@Override
		public boolean isVerifyPeer() {
			return verifyPeer;
		}

		void setVerifyPeer(boolean verifyPeer) {
			this.verifyPeer = verifyPeer;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isStartTls()
		 */
		@Override
		public boolean isStartTls() {
			return startTls;
		}

		void setStartTls(boolean startTls) {
			this.startTls = startTls;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientResources()
		 */
		@Override
		public Optional<ClientResources> getClientResources() {
			return Optional.ofNullable(clientResources);
		}

		void setClientResources(ClientResources clientResources) {
			this.clientResources = clientResources;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientOptions()
		 */
		@Override
		public Optional<ClientOptions> getClientOptions() {
			return Optional.empty();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getReadFrom()
		 */
		@Override
		public Optional<ReadFrom> getReadFrom() {
			return Optional.empty();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientName()
		 */
		@Override
		public Optional<String> getClientName() {
			return Optional.ofNullable(clientName);
		}

		/**
		 * @param clientName can be {@literal null}.
		 * @since 2.1
		 */
		void setClientName(@Nullable String clientName) {
			this.clientName = clientName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getTimeout()
		 */
		@Override
		public Duration getCommandTimeout() {
			return timeout;
		}

		void setTimeout(Duration timeout) {
			this.timeout = timeout;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getShutdownTimeout()
		 */
		@Override
		public Duration getShutdownTimeout() {
			return shutdownTimeout;
		}

		void setShutdownTimeout(Duration shutdownTimeout) {
			this.shutdownTimeout = shutdownTimeout;
		}
	}
}
