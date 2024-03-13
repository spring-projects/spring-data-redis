/*
 * Copyright 2011-2024 the original author or authors.
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

import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.Pool;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration.WithDatabaseIndex;
import org.springframework.data.redis.connection.RedisConfiguration.WithPassword;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterTopologyProvider;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Connection factory creating <a href="https://github.com/redis/jedis">Jedis</a> based connections.
 * <p>
 * {@link JedisConnectionFactory} should be configured using an environmental configuration and the
 * {@link JedisClientConfiguration client configuration}. Jedis supports the following environmental configurations:
 * <ul>
 * <li>{@link RedisStandaloneConfiguration}</li>
 * <li>{@link RedisSentinelConfiguration}</li>
 * <li>{@link RedisClusterConfiguration}</li>
 * </ul>
 * <p>
 * This connection factory implements {@link InitializingBean} and {@link SmartLifecycle} for flexible lifecycle
 * control. It must be {@link #afterPropertiesSet() initialized} and {@link #start() started} before you can obtain a
 * connection. {@link #afterPropertiesSet() Initialization} {@link SmartLifecycle#start() starts} this bean
 * {@link #isEarlyStartup() early} by default. You can {@link SmartLifecycle#stop()} and {@link SmartLifecycle#start()
 * restart} this connection factory if needed. Disabling {@link #isEarlyStartup() early startup} leaves lifecycle
 * management to the container refresh if {@link #isAutoStartup() auto-startup} is enabled.
 * <p>
 * Note that {@link JedisConnection} and its {@link JedisClusterConnection clustered variant} are not Thread-safe and
 * instances should not be shared across threads. Refer to the
 * <a href="https://github.com/redis/jedis/wiki/Getting-started#using-jedis-in-a-multithreaded-environment">Jedis
 * documentation</a> for guidance on configuring Jedis in a multithreaded environment.
 *
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Fu Jian
 * @author Ajith Kumar
 * @see JedisClientConfiguration
 * @see Jedis
 */
public class JedisConnectionFactory
		implements RedisConnectionFactory, InitializingBean, DisposableBean, SmartLifecycle {

	private static final Log log = LogFactory.getLog(JedisConnectionFactory.class);

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private int phase = 0; // in between min and max values
	private boolean autoStartup = true;
	private boolean earlyStartup = true;
	private boolean convertPipelineAndTxResults = true;

	private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

	private @Nullable ClusterCommandExecutor clusterCommandExecutor;

	private @Nullable AsyncTaskExecutor executor;

	private @Nullable ClusterTopologyProvider topologyProvider;

	private JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().build();

	private final JedisClientConfiguration clientConfiguration;

	private @Nullable JedisCluster cluster;

	private @Nullable Pool<Jedis> pool;

	private @Nullable RedisConfiguration configuration;

	private RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration("localhost",
			Protocol.DEFAULT_PORT);

	/**
	 * Lifecycle state of this factory.
	 */
	enum State {
		CREATED, STARTING, STARTED, STOPPING, STOPPED, DESTROYED;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance with default settings (default connection pooling).
	 */
	public JedisConnectionFactory() {
		this(new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance given {@link JedisClientConfiguration}.
	 *
	 * @param clientConfiguration must not be {@literal null}
	 * @since 2.0
	 */
	private JedisConnectionFactory(JedisClientConfiguration clientConfiguration) {

		Assert.notNull(clientConfiguration, "JedisClientConfiguration must not be null");

		this.clientConfiguration = clientConfiguration;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given pool configuration.
	 *
	 * @param poolConfig pool configuration
	 */
	public JedisConnectionFactory(JedisPoolConfig poolConfig) {
		this((RedisSentinelConfiguration) null, poolConfig);
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @since 1.7
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfiguration) {
		this(clusterConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(clusterConfiguration, "RedisClusterConfiguration must not be null");

		this.configuration = clusterConfiguration;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @since 1.7
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfiguration, JedisPoolConfig poolConfig) {

		Assert.notNull(clusterConfiguration, "RedisClusterConfiguration must not be null");

		this.configuration = clusterConfiguration;
		this.clientConfiguration = MutableJedisClientConfiguration.create(poolConfig);
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 * @since 1.4
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfiguration) {
		this(sentinelConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisSentinelConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(sentinelConfiguration, "RedisSentinelConfiguration must not be null");

		this.configuration = sentinelConfiguration;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 *
	 * @param sentinelConfiguration the sentinel configuration to use.
	 * @param poolConfig pool configuration. Defaulted to new instance if {@literal null}.
	 * @since 1.4
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfiguration,
			@Nullable JedisPoolConfig poolConfig) {

		this.configuration = sentinelConfiguration;
		this.clientConfiguration = MutableJedisClientConfiguration
				.create(poolConfig != null ? poolConfig : new JedisPoolConfig());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisStandaloneConfiguration}.
	 *
	 * @param standaloneConfiguration must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisStandaloneConfiguration standaloneConfiguration) {
		this(standaloneConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisStandaloneConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param standaloneConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisStandaloneConfiguration standaloneConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(standaloneConfiguration, "RedisStandaloneConfiguration must not be null");

		this.standaloneConfig = standaloneConfiguration;
	}

	ClusterCommandExecutor getRequiredClusterCommandExecutor() {

		if (this.clusterCommandExecutor == null) {
			throw new IllegalStateException("ClusterCommandExecutor not initialized");
		}

		return this.clusterCommandExecutor;
	}

	/**
	 * Configures the {@link AsyncTaskExecutor executor} used to execute commands asynchronously across the cluster.
	 *
	 * @param executor {@link AsyncTaskExecutor executor} used to execute commands asynchronously across the cluster.
	 * @since 3.2
	 */
	public void setExecutor(AsyncTaskExecutor executor) {

		Assert.notNull(executor, "AsyncTaskExecutor must not be null");

		this.executor = executor;
	}

	/**
	 * Returns the Redis hostname.
	 *
	 * @return the hostName.
	 */
	public String getHostName() {
		return standaloneConfig.getHostName();
	}

	/**
	 * Sets the Redis hostname.
	 *
	 * @param hostName the hostname to set.
	 * @deprecated since 2.0, configure the hostname using {@link RedisStandaloneConfiguration}.
	 */
	@Deprecated
	public void setHostName(String hostName) {
		standaloneConfig.setHostName(hostName);
	}

	/**
	 * Returns whether to use SSL.
	 *
	 * @return use of SSL.
	 * @since 1.8
	 */
	public boolean isUseSsl() {
		return clientConfiguration.isUseSsl();
	}

	/**
	 * Sets whether to use SSL.
	 *
	 * @param useSsl {@literal true} to use SSL.
	 * @since 1.8
	 * @deprecated since 2.0, configure the SSL usage with {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setUseSsl(boolean useSsl) {
		getMutableConfiguration().setUseSsl(useSsl);
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 *
	 * @return password for authentication.
	 */
	@Nullable
	public String getPassword() {
		return getRedisPassword().map(String::new).orElse(null);
	}

	@Nullable
	private String getRedisUsername() {
		return RedisConfiguration.getUsernameOrElse(this.configuration, standaloneConfig::getUsername);
	}

	private RedisPassword getRedisPassword() {
		return RedisConfiguration.getPasswordOrElse(this.configuration, standaloneConfig::getPassword);
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 *
	 * @param password the password to set.
	 * @deprecated since 2.0, configure the password using {@link RedisStandaloneConfiguration},
	 *             {@link RedisSentinelConfiguration} or {@link RedisClusterConfiguration}.
	 */
	@Deprecated
	public void setPassword(String password) {

		if (RedisConfiguration.isAuthenticationAware(configuration)) {

			((WithPassword) configuration).setPassword(password);
			return;
		}

		standaloneConfig.setPassword(RedisPassword.of(password));
	}

	/**
	 * Returns the port used to connect to the Redis instance.
	 *
	 * @return the Redis port.
	 */
	public int getPort() {
		return standaloneConfig.getPort();
	}

	/**
	 * Sets the port used to connect to the Redis instance.
	 *
	 * @param port the Redis port.
	 * @deprecated since 2.0, configure the port using {@link RedisStandaloneConfiguration}.
	 */
	@Deprecated
	public void setPort(int port) {
		standaloneConfig.setPort(port);
	}

	/**
	 * Returns the timeout.
	 *
	 * @return the timeout.
	 */
	public int getTimeout() {
		return getReadTimeout();
	}

	/**
	 * Sets the timeout.
	 *
	 * @param timeout the timeout to set.
	 * @deprecated since 2.0, configure the timeout using {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setTimeout(int timeout) {

		getMutableConfiguration().setReadTimeout(Duration.ofMillis(timeout));
		getMutableConfiguration().setConnectTimeout(Duration.ofMillis(timeout));
	}

	/**
	 * Indicates the use of a connection pool.
	 * <p>
	 * Applies only to single node Redis. Sentinel and Cluster modes use always connection-pooling regardless of the
	 * pooling setting.
	 *
	 * @return the use of connection pooling.
	 */
	public boolean getUsePool() {
		// Jedis Sentinel cannot operate without a pool.
		return isRedisSentinelAware() || getClientConfiguration().isUsePooling();
	}

	/**
	 * Turns on or off the use of connection pooling.
	 *
	 * @param usePool the usePool to set.
	 * @deprecated since 2.0, configure pooling usage with {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 * @throws IllegalStateException if configured to use sentinel and {@code usePool} is {@literal false} as Jedis
	 *           requires pooling for Redis sentinel use.
	 */
	@Deprecated
	public void setUsePool(boolean usePool) {

		if (isRedisSentinelAware() && !usePool) {
			throw new IllegalStateException("Jedis requires pooling for Redis Sentinel use");
		}

		getMutableConfiguration().setUsePooling(usePool);
	}

	/**
	 * Returns the poolConfig.
	 *
	 * @return the poolConfig
	 */
	@Nullable
	public <T> GenericObjectPoolConfig<T> getPoolConfig() {
		return clientConfiguration.getPoolConfig().orElse(null);
	}

	/**
	 * Sets the pool configuration for this factory.
	 *
	 * @param poolConfig the poolConfig to set.
	 * @deprecated since 2.0, configure {@link JedisPoolConfig} using {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setPoolConfig(JedisPoolConfig poolConfig) {
		getMutableConfiguration().setPoolConfig(poolConfig);
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
	 * @param index database index.
	 * @deprecated since 2.0, configure the database index using {@link RedisStandaloneConfiguration} or
	 *             {@link RedisSentinelConfiguration}.
	 */
	@Deprecated
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
	 * @return the client name.
	 * @since 1.8
	 */
	@Nullable
	public String getClientName() {
		return clientConfiguration.getClientName().orElse(null);
	}

	/**
	 * Sets the client name used by this connection factory. Defaults to none which does not set a client name.
	 *
	 * @param clientName the client name.
	 * @since 1.8
	 * @deprecated since 2.0, configure the client name using {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setClientName(String clientName) {
		this.getMutableConfiguration().setClientName(clientName);
	}

	/**
	 * @return the {@link JedisClientConfiguration}.
	 * @since 2.0
	 */
	public JedisClientConfiguration getClientConfiguration() {
		return this.clientConfiguration;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}.
	 * @since 2.0
	 */
	@Nullable
	public RedisStandaloneConfiguration getStandaloneConfiguration() {
		return this.standaloneConfig;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}, may be {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	public RedisSentinelConfiguration getSentinelConfiguration() {
		return RedisConfiguration.isSentinelConfiguration(configuration) ? (RedisSentinelConfiguration) configuration
				: null;
	}

	/**
	 * @return the {@link RedisClusterConfiguration}, may be {@literal null}.
	 * @since 2.0
	 */
	@Nullable
	public RedisClusterConfiguration getClusterConfiguration() {
		return RedisConfiguration.isClusterConfiguration(configuration) ? (RedisClusterConfiguration) configuration : null;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Specify the lifecycle phase for pausing and resuming this executor. The default is {@code 0}.
	 *
	 * @since 3.2
	 * @see SmartLifecycle#getPhase()
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * @since 3.3
	 */
	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Configure if this Lifecycle connection factory should get started automatically by the container at the time that
	 * the containing ApplicationContext gets refreshed.
	 * <p>
	 * This connection factory defaults to early auto-startup during {@link #afterPropertiesSet()} and can potentially
	 * create Redis connections early on in the lifecycle. See {@link #setEarlyStartup(boolean)} for delaying connection
	 * creation to the ApplicationContext refresh if auto-startup is enabled.
	 *
	 * @param autoStartup {@literal true} to automatically {@link #start()} the connection factory; {@literal false}
	 *          otherwise.
	 * @since 3.3
	 * @see #setEarlyStartup(boolean)
	 * @see #start()
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @return whether to {@link #start()} the component during {@link #afterPropertiesSet()}.
	 * @since 3.3
	 */
	public boolean isEarlyStartup() {
		return this.earlyStartup;
	}

	/**
	 * Configure if this InitializingBean's component Lifecycle should get started early by {@link #afterPropertiesSet()}
	 * at the time that the bean is initialized. The component defaults to auto-startup.
	 * <p>
	 * This method is related to {@link #setAutoStartup(boolean) auto-startup} and can be used to delay Redis client
	 * startup until the ApplicationContext refresh. Disabling early startup does not disable auto-startup.
	 *
	 * @param earlyStartup {@literal true} to early {@link #start()} the component; {@literal false} otherwise.
	 * @since 3.3
	 * @see #setAutoStartup(boolean)
	 */
	public void setEarlyStartup(boolean earlyStartup) {
		this.earlyStartup = earlyStartup;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If {@code false}, results of
	 * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
	 * Jedis driver.
	 *
	 * @return {@code true} to convert pipeline and transaction results; {@code false} otherwise.
	 */
	@Override
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If {@code false}, results of
	 * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
	 * Jedis driver.
	 *
	 * @param convertPipelineAndTxResults {@code true} to convert pipeline and transaction results; {@code false}
	 *          otherwise.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/**
	 * @return true when {@link RedisSentinelConfiguration} is present.
	 * @since 1.4
	 */
	public boolean isRedisSentinelAware() {
		return RedisConfiguration.isSentinelConfiguration(configuration);
	}

	/**
	 * @return true when {@link RedisClusterConfiguration} is present.
	 * @since 2.0
	 */
	public boolean isRedisClusterAware() {
		return RedisConfiguration.isClusterConfiguration(configuration);
	}

	@Override
	public void afterPropertiesSet() {

		this.clientConfig = createClientConfig(getDatabase(), getRedisUsername(), getRedisPassword());

		if (isEarlyStartup()) {
			start();
		}
	}

	private JedisClientConfig createClientConfig(int database, @Nullable String username, RedisPassword password) {

		DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder();

		this.clientConfiguration.getClientName().ifPresent(builder::clientName);
		builder.connectionTimeoutMillis(getConnectTimeout());
		builder.socketTimeoutMillis(getReadTimeout());

		builder.database(database);

		if (!ObjectUtils.isEmpty(username)) {
			builder.user(username);
		}
		password.toOptional().map(String::new).ifPresent(builder::password);

		if (isUseSsl()) {

			builder.ssl(true);

			this.clientConfiguration.getSslSocketFactory().ifPresent(builder::sslSocketFactory);
			this.clientConfiguration.getHostnameVerifier().ifPresent(builder::hostnameVerifier);
			this.clientConfiguration.getSslParameters().ifPresent(builder::sslParameters);
		}

		return builder.build();
	}

	JedisClientConfig createSentinelClientConfig(SentinelConfiguration sentinelConfiguration) {
		return createClientConfig(0, sentinelConfiguration.getSentinelUsername(),
				sentinelConfiguration.getSentinelPassword());
	}

	@Override
	public void start() {

		State current = this.state.getAndUpdate(state -> isCreatedOrStopped(state) ? State.STARTING : state);

		if (isCreatedOrStopped(current)) {

			if (getUsePool() && !isRedisClusterAware()) {
				this.pool = createPool();
			}

			if (isRedisClusterAware()) {

				this.cluster = createCluster(getClusterConfiguration(), getPoolConfig());
				this.topologyProvider = createTopologyProvider(this.cluster);
				this.clusterCommandExecutor = new ClusterCommandExecutor(this.topologyProvider,
						new JedisClusterConnection.JedisClusterNodeResourceProvider(this.cluster, this.topologyProvider),
						EXCEPTION_TRANSLATION, executor);
			}

			this.state.set(State.STARTED);
		}
	}

	private boolean isCreatedOrStopped(@Nullable State state) {
		return State.CREATED.equals(state) || State.STOPPED.equals(state);
	}

	@Override
	public void stop() {

		if (this.state.compareAndSet(State.STARTED, State.STOPPING)) {

			if (getUsePool() && !isRedisClusterAware()) {
				if (this.pool != null) {
					try {
						this.pool.close();
						this.pool = null;
					} catch (Exception ex) {
						log.warn("Cannot properly close Jedis pool", ex);
					}
				}
			}

			ClusterCommandExecutor clusterCommandExecutor = this.clusterCommandExecutor;

			if (clusterCommandExecutor != null) {
				try {
					clusterCommandExecutor.destroy();
					this.clusterCommandExecutor = null;
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			if (this.cluster != null) {

				this.topologyProvider = null;

				try {
					this.cluster.close();
					this.cluster = null;
				} catch (Exception ex) {
					log.warn("Cannot properly close Jedis cluster", ex);
				}
			}

			this.state.set(State.STOPPED);
		}
	}

	@Override
	public boolean isRunning() {
		return State.STARTED.equals(this.state.get());
	}

	private Pool<Jedis> createPool() {

		if (isRedisSentinelAware()) {
			return createRedisSentinelPool(getSentinelConfiguration());
		}
		return createRedisPool();
	}

	/**
	 * Creates {@link JedisSentinelPool}.
	 *
	 * @param config the actual {@link RedisSentinelConfiguration}. Never {@literal null}.
	 * @return the {@link Pool} to use. Never {@literal null}.
	 * @since 1.4
	 */
	protected Pool<Jedis> createRedisSentinelPool(RedisSentinelConfiguration config) {

		GenericObjectPoolConfig<Jedis> poolConfig = getPoolConfig() != null ? getPoolConfig() : new JedisPoolConfig();

		JedisClientConfig sentinelConfig = createSentinelClientConfig(config);

		return new JedisSentinelPool(config.getMaster().getName(), convertToJedisSentinelSet(config.getSentinels()),
				poolConfig, this.clientConfig, sentinelConfig);
	}

	/**
	 * Creates {@link JedisPool}.
	 *
	 * @return the {@link Pool} to use. Never {@literal null}.
	 * @since 1.4
	 */
	protected Pool<Jedis> createRedisPool() {
		return new JedisPool(getPoolConfig(), new HostAndPort(getHostName(), getPort()), this.clientConfig);
	}

	/**
	 * Template method to create a {@link ClusterTopologyProvider} given {@link JedisCluster}. Creates
	 * {@link JedisClusterTopologyProvider} by default.
	 *
	 * @param cluster the {@link JedisCluster}, must not be {@literal null}.
	 * @return the {@link ClusterTopologyProvider}.
	 * @see JedisClusterTopologyProvider
	 * @see 2.2
	 */
	protected ClusterTopologyProvider createTopologyProvider(JedisCluster cluster) {
		return new JedisClusterTopologyProvider(cluster);
	}

	/**
	 * Creates {@link JedisCluster} for given {@link RedisClusterConfiguration} and {@link GenericObjectPoolConfig}.
	 *
	 * @param clusterConfig must not be {@literal null}.
	 * @param poolConfig can be {@literal null}.
	 * @return the actual {@link JedisCluster}.
	 * @since 1.7
	 */
	protected JedisCluster createCluster(RedisClusterConfiguration clusterConfig,
			GenericObjectPoolConfig<Connection> poolConfig) {

		Assert.notNull(clusterConfig, "Cluster configuration must not be null");

		Set<HostAndPort> hostAndPort = new HashSet<>();

		for (RedisNode node : clusterConfig.getClusterNodes()) {
			hostAndPort.add(new HostAndPort(node.getHost(), node.getPort()));
		}

		int redirects = clusterConfig.getMaxRedirects() != null ? clusterConfig.getMaxRedirects() : 5;

		return new JedisCluster(hostAndPort, this.clientConfig, redirects, poolConfig);
	}

	@Override
	public void destroy() {

		stop();
		state.set(State.DESTROYED);
	}

	@Override
	public RedisConnection getConnection() {

		assertInitialized();

		if (isRedisClusterAware()) {
			return getClusterConnection();
		}

		Jedis jedis = fetchJedisConnector();
		JedisClientConfig sentinelConfig = this.clientConfig;

		SentinelConfiguration sentinelConfiguration = getSentinelConfiguration();

		if (sentinelConfiguration != null) {
			sentinelConfig = createSentinelClientConfig(sentinelConfiguration);
		}

		JedisConnection connection = getUsePool() ? new JedisConnection(jedis, this.pool, this.clientConfig, sentinelConfig)
				: new JedisConnection(jedis, null, this.clientConfig, sentinelConfig);

		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);

		return postProcessConnection(connection);
	}

	/**
	 * Returns a Jedis instance to be used as a Redis connection. The instance can be newly created or retrieved from a
	 * pool.
	 *
	 * @return Jedis instance ready for wrapping into a {@link RedisConnection}.
	 */
	protected Jedis fetchJedisConnector() {

		try {

			if (getUsePool() && this.pool != null) {
				return this.pool.getResource();
			}

			Jedis jedis = createJedis();

			// force initialization (see Jedis issue #82)
			jedis.connect();

			return jedis;
		} catch (Exception ex) {
			throw new RedisConnectionFailureException("Cannot get Jedis connection", ex);
		}
	}

	private Jedis createJedis() {
		return new Jedis(new HostAndPort(getHostName(), getPort()), this.clientConfig);
	}

	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing initialization commands on a new
	 * connection. This implementation simply returns the connection.
	 *
	 * @param connection the jedis connection.
	 * @return processed connection
	 */
	protected JedisConnection postProcessConnection(JedisConnection connection) {
		return connection;
	}

	@Override
	public RedisClusterConnection getClusterConnection() {

		assertInitialized();

		if (!isRedisClusterAware()) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured");
		}

		JedisClusterConnection clusterConnection = new JedisClusterConnection(this.cluster,
				getRequiredClusterCommandExecutor(), this.topologyProvider);

		return postProcessConnection(clusterConnection);
	}

	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing initialization commands on a new
	 * connection. This implementation simply returns the connection.
	 *
	 * @param connection the jedis connection.
	 * @return processed connection.
	 * @since 3.2
	 */
	protected JedisClusterConnection postProcessConnection(JedisClusterConnection connection) {
		return connection;
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {

		assertInitialized();

		if (!isRedisSentinelAware()) {
			throw new InvalidDataAccessResourceUsageException("No Sentinels configured");
		}

		return new JedisSentinelConnection(getActiveSentinel());
	}

	private Jedis getActiveSentinel() {

		Assert.isTrue(RedisConfiguration.isSentinelConfiguration(configuration), "SentinelConfig must not be null");
		SentinelConfiguration sentinelConfiguration = (SentinelConfiguration) configuration;

		JedisClientConfig clientConfig = createSentinelClientConfig(sentinelConfiguration);
		for (RedisNode node : sentinelConfiguration.getSentinels()) {

			Jedis jedis = null;
			boolean success = false;

			try {

				jedis = new Jedis(new HostAndPort(node.getHost(), node.getPort()), clientConfig);
				if (jedis.ping().equalsIgnoreCase("pong")) {
					success = true;
					return jedis;
				}
			} catch (Exception ex) {
				log.warn(String.format("Ping failed for sentinel host: %s", node.getHost()), ex);
			} finally {
				if (!success && jedis != null) {
					jedis.close();
				}
			}
		}

		throw new InvalidDataAccessResourceUsageException("No Sentinel found");
	}

	private static Set<HostAndPort> convertToJedisSentinelSet(Collection<RedisNode> nodes) {

		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptySet();
		}

		Set<HostAndPort> convertedNodes = new LinkedHashSet<>(nodes.size());
		for (RedisNode node : nodes) {
			if (node != null) {
				convertedNodes.add(new HostAndPort(node.getHost(), node.getPort()));
			}
		}
		return convertedNodes;
	}

	private int getReadTimeout() {
		return Math.toIntExact(clientConfiguration.getReadTimeout().toMillis());
	}

	private int getConnectTimeout() {
		return Math.toIntExact(clientConfiguration.getConnectTimeout().toMillis());
	}

	private MutableJedisClientConfiguration getMutableConfiguration() {

		Assert.state(clientConfiguration instanceof MutableJedisClientConfiguration,
				() -> String.format("Client configuration must be instance of MutableJedisClientConfiguration but is %s",
						ClassUtils.getShortName(clientConfiguration.getClass())));

		return (MutableJedisClientConfiguration) clientConfiguration;
	}

	private void assertInitialized() {

		State current = state.get();

		if (State.STARTED.equals(current)) {
			return;
		}

		switch (current) {
			case CREATED, STOPPED -> throw new IllegalStateException(
					String.format("JedisConnectionFactory has been %s. Use start() to initialize it", current));
			case DESTROYED ->
				throw new IllegalStateException("JedisConnectionFactory was destroyed and cannot be used anymore");
			default -> throw new IllegalStateException(String.format("JedisConnectionFactory is %s", current));
		}
	}

	/**
	 * Mutable implementation of {@link JedisClientConfiguration}.
	 *
	 * @author Mark Paluch
	 */
	static class MutableJedisClientConfiguration implements JedisClientConfiguration {

		private boolean useSsl;
		private @Nullable SSLSocketFactory sslSocketFactory;
		private @Nullable SSLParameters sslParameters;
		private @Nullable HostnameVerifier hostnameVerifier;
		private boolean usePooling = true;
		private GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		private @Nullable String clientName;
		private Duration readTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);
		private Duration connectTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);

		public static JedisClientConfiguration create(GenericObjectPoolConfig jedisPoolConfig) {

			MutableJedisClientConfiguration configuration = new MutableJedisClientConfiguration();
			configuration.setPoolConfig(jedisPoolConfig);
			return configuration;
		}

		@Override
		public boolean isUseSsl() {
			return useSsl;
		}

		public void setUseSsl(boolean useSsl) {
			this.useSsl = useSsl;
		}

		@Override
		public Optional<SSLSocketFactory> getSslSocketFactory() {
			return Optional.ofNullable(sslSocketFactory);
		}

		public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
			this.sslSocketFactory = sslSocketFactory;
		}

		@Override
		public Optional<SSLParameters> getSslParameters() {
			return Optional.ofNullable(sslParameters);
		}

		public void setSslParameters(SSLParameters sslParameters) {
			this.sslParameters = sslParameters;
		}

		@Override
		public Optional<HostnameVerifier> getHostnameVerifier() {
			return Optional.ofNullable(hostnameVerifier);
		}

		public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
			this.hostnameVerifier = hostnameVerifier;
		}

		@Override
		public boolean isUsePooling() {
			return usePooling;
		}

		public void setUsePooling(boolean usePooling) {
			this.usePooling = usePooling;
		}

		@Override
		public Optional<GenericObjectPoolConfig> getPoolConfig() {
			return Optional.ofNullable(poolConfig);
		}

		public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
			this.poolConfig = poolConfig;
		}

		@Override
		public Optional<String> getClientName() {
			return Optional.ofNullable(clientName);
		}

		public void setClientName(String clientName) {
			this.clientName = clientName;
		}

		@Override
		public Duration getReadTimeout() {
			return readTimeout;
		}

		public void setReadTimeout(Duration readTimeout) {
			this.readTimeout = readTimeout;
		}

		@Override
		public Duration getConnectTimeout() {
			return connectTimeout;
		}

		public void setConnectTimeout(Duration connectTimeout) {
			this.connectTimeout = connectTimeout;
		}
	}
}
