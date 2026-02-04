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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.RedisConfiguration.SentinelConfiguration;
import org.springframework.data.redis.util.RedisClientLibraryInfo;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import redis.clients.jedis.*;

import static org.springframework.data.redis.connection.jedis.JedisConnectionFactory.MutableJedisClientConfiguration;

/**
 * Connection factory creating connections based on the Jedis 7.2+ {@link RedisClient} API.
 * <p>
 * This factory uses the new {@link RedisClient} class introduced in Jedis 7.2.0, which provides built-in connection
 * pooling and improved resource management.
 * <p>
 * {@link JedisClientConnectionFactory} can be configured using:
 * <ul>
 * <li>{@link RedisStandaloneConfiguration} for standalone Redis (fully supported)</li>
 * <li>{@link RedisSentinelConfiguration} for Redis Sentinel (constructors available, connection implementation
 * pending)</li>
 * <li>{@link RedisClusterConfiguration} for Redis Cluster (constructors available, connection implementation
 * pending)</li>
 * </ul>
 * <p>
 * This connection factory implements {@link InitializingBean} and {@link SmartLifecycle} for flexible lifecycle
 * control. It must be {@link #afterPropertiesSet() initialized} and {@link #start() started} before you can obtain a
 * connection.
 * <p>
 * Note that {@link JedisClientConnection} and its {@link JedisClientClusterConnection clustered variant} are not
 * Thread-safe and instances should not be shared across threads. Refer to the
 * <a href="https://github.com/redis/jedis/wiki/Getting-started#using-jedis-in-a-multithreaded-environment">Jedis
 * documentation</a> for guidance on configuring Jedis in a multithreaded environment.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see RedisClient
 * @see JedisClientConfiguration
 * @see JedisConnectionFactory
 */
public class JedisClientConnectionFactory
		implements RedisConnectionFactory, InitializingBean, DisposableBean, SmartLifecycle {

	private static final Log log = LogFactory.getLog(JedisClientConnectionFactory.class);

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private int phase = 0;
	private boolean autoStartup = true;
	private boolean earlyStartup = true;
	private boolean convertPipelineAndTxResults = true;

	private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

	private final JedisClientConfiguration clientConfiguration;
	private JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().build();

	private @Nullable RedisClient redisClient;
	private @Nullable RedisSentinelClient sentinelClient;
	private @Nullable RedisClusterClient clusterClient;
	private @Nullable RedisConfiguration configuration;

	private @Nullable ClusterTopologyProvider topologyProvider;
	private @Nullable ClusterCommandExecutor clusterCommandExecutor;
	private AsyncTaskExecutor executor = new org.springframework.core.task.SimpleAsyncTaskExecutor();

	private RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration("localhost",
			Protocol.DEFAULT_PORT);

	/**
	 * Lifecycle state of this factory.
	 */
	enum State {
		CREATED, STARTING, STARTED, STOPPING, STOPPED, DESTROYED
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance with default settings.
	 */
	public JedisClientConnectionFactory() {
		this(new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance given {@link JedisClientConfiguration}.
	 *
	 * @param clientConfiguration must not be {@literal null}
	 */
	private JedisClientConnectionFactory(JedisClientConfiguration clientConfiguration) {

		Assert.notNull(clientConfiguration, "JedisClientConfiguration must not be null");

		this.clientConfiguration = clientConfiguration;
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given
	 * {@link RedisStandaloneConfiguration}.
	 *
	 * @param standaloneConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisStandaloneConfiguration standaloneConfiguration) {
		this(standaloneConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given {@link RedisStandaloneConfiguration}
	 * and {@link JedisClientConfiguration}.
	 *
	 * @param standaloneConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisStandaloneConfiguration standaloneConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(standaloneConfiguration, "RedisStandaloneConfiguration must not be null");

		this.standaloneConfig = standaloneConfiguration;
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given {@link RedisSentinelConfiguration}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisSentinelConfiguration sentinelConfiguration) {
		this(sentinelConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given {@link RedisSentinelConfiguration}
	 * and {@link JedisClientConfiguration}.
	 *
	 * @param sentinelConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisSentinelConfiguration sentinelConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(sentinelConfiguration, "RedisSentinelConfiguration must not be null");

		this.configuration = sentinelConfiguration;
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given {@link RedisClusterConfiguration}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisClusterConfiguration clusterConfiguration) {
		this(clusterConfiguration, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisClientConnectionFactory} instance using the given {@link RedisClusterConfiguration}
	 * and {@link JedisClientConfiguration}.
	 *
	 * @param clusterConfiguration must not be {@literal null}.
	 * @param clientConfiguration must not be {@literal null}.
	 */
	public JedisClientConnectionFactory(RedisClusterConfiguration clusterConfiguration,
			JedisClientConfiguration clientConfiguration) {

		this(clientConfiguration);

		Assert.notNull(clusterConfiguration, "RedisClusterConfiguration must not be null");

		this.configuration = clusterConfiguration;
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
	 * Returns the port used to connect to the Redis instance.
	 *
	 * @return the Redis port.
	 */
	public int getPort() {
		return standaloneConfig.getPort();
	}

	/**
	 * Returns the index of the database.
	 *
	 * @return the database index.
	 */
	public int getDatabase() {
		return standaloneConfig.getDatabase();
	}

	private @Nullable String getRedisUsername() {
		return standaloneConfig.getUsername();
	}

	private RedisPassword getRedisPassword() {
		return standaloneConfig.getPassword();
	}

	/**
	 * @return the {@link JedisClientConfiguration}.
	 */
	public JedisClientConfiguration getClientConfiguration() {
		return this.clientConfiguration;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}.
	 */
	public RedisStandaloneConfiguration getStandaloneConfiguration() {
		return this.standaloneConfig;
	}

	/**
	 * @return the {@link RedisSentinelConfiguration} or {@literal null} if not configured.
	 */
	public @Nullable RedisSentinelConfiguration getSentinelConfiguration() {
		return RedisConfiguration.isSentinelConfiguration(configuration) ? (RedisSentinelConfiguration) configuration
				: null;
	}

	/**
	 * @return the {@link RedisClusterConfiguration} or {@literal null} if not configured.
	 */
	public @Nullable RedisClusterConfiguration getClusterConfiguration() {
		return RedisConfiguration.isClusterConfiguration(configuration) ? (RedisClusterConfiguration) configuration : null;
	}

	/**
	 * @return true when {@link RedisSentinelConfiguration} is present.
	 */
	public boolean isRedisSentinelAware() {
		return RedisConfiguration.isSentinelConfiguration(configuration);
	}

	/**
	 * @return true when {@link RedisClusterConfiguration} is present.
	 */
	public boolean isRedisClusterAware() {
		return RedisConfiguration.isClusterConfiguration(configuration);
	}

	/**
	 * Returns the client name.
	 *
	 * @return the client name.
	 */
	public @Nullable String getClientName() {
		return clientConfiguration.getClientName().orElse(null);
	}

	/**
	 * Returns whether SSL is enabled.
	 *
	 * @return {@literal true} if SSL is enabled.
	 */
	public boolean isUseSsl() {
		return clientConfiguration.isUseSsl();
	}

	/**
	 * Returns the read timeout in milliseconds.
	 *
	 * @return the read timeout in milliseconds.
	 */
	public int getTimeout() {
		return (int) clientConfiguration.getReadTimeout().toMillis();
	}

	/**
	 * Returns whether connection pooling is enabled.
	 *
	 * @return {@literal true} if connection pooling is enabled.
	 */
	public boolean getUsePool() {
		return clientConfiguration.isUsePooling();
	}

	/**
	 * Sets the async task executor for cluster command execution.
	 *
	 * @param executor the executor to use for async cluster commands.
	 */
	public void setExecutor(AsyncTaskExecutor executor) {
		this.executor = executor;
	}

	/**
	 * Returns the cluster command executor.
	 *
	 * @return the cluster command executor.
	 * @throws IllegalStateException if the factory is not in cluster mode or not started.
	 */
	ClusterCommandExecutor getRequiredClusterCommandExecutor() {
		if (clusterCommandExecutor == null) {
			throw new IllegalStateException(
					"ClusterCommandExecutor is not available. Ensure the factory is in cluster mode and has been started.");
		}
		return clusterCommandExecutor;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Specify the lifecycle phase for pausing and resuming this executor. The default is {@code 0}.
	 *
	 * @see SmartLifecycle#getPhase()
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Configure if this Lifecycle connection factory should get started automatically by the container.
	 *
	 * @param autoStartup {@literal true} to automatically {@link #start()} the connection factory.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @return whether to {@link #start()} the component during {@link #afterPropertiesSet()}.
	 */
	public boolean isEarlyStartup() {
		return this.earlyStartup;
	}

	/**
	 * Configure if this InitializingBean's component Lifecycle should get started early.
	 *
	 * @param earlyStartup {@literal true} to early {@link #start()} the component.
	 */
	public void setEarlyStartup(boolean earlyStartup) {
		this.earlyStartup = earlyStartup;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type.
	 *
	 * @return {@code true} to convert pipeline and transaction results.
	 */
	@Override
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type.
	 *
	 * @param convertPipelineAndTxResults {@code true} to convert pipeline and transaction results.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
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

		builder.clientSetInfoConfig(new ClientSetInfoConfig(DriverInfo.builder()
				.addUpstreamDriver(RedisClientLibraryInfo.FRAMEWORK_NAME, RedisClientLibraryInfo.getVersion()).build()));

		builder.database(database);

		if (!ObjectUtils.isEmpty(username)) {
			builder.user(username);
		}
		password.toOptional().map(String::new).ifPresent(builder::password);

		if (clientConfiguration.isUseSsl()) {

			builder.ssl(true);

			this.clientConfiguration.getSslSocketFactory().ifPresent(builder::sslSocketFactory);
			this.clientConfiguration.getHostnameVerifier().ifPresent(builder::hostnameVerifier);
			this.clientConfiguration.getSslParameters().ifPresent(builder::sslParameters);
		}

		this.clientConfiguration.getCustomizer().ifPresent(customizer -> customizer.customize(builder));

		return builder.build();
	}

	@Override
	@SuppressWarnings("NullAway")
	public void start() {

		State current = this.state.getAndUpdate(state -> isCreatedOrStopped(state) ? State.STARTING : state);

		if (isCreatedOrStopped(current)) {
			if (isRedisSentinelAware()) {
				this.sentinelClient = createRedisSentinelClient();
			} else if (isRedisClusterAware()) {
				this.clusterClient = createRedisClusterClient();
				this.topologyProvider = createTopologyProvider(this.clusterClient);
				this.clusterCommandExecutor = createClusterCommandExecutor(this.topologyProvider);
			} else {
				this.redisClient = createRedisClient();
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

			dispose(redisClient);
			redisClient = null;

			disposeSentinel(sentinelClient);
			sentinelClient = null;

			disposeCluster(clusterClient);
			clusterClient = null;

			this.state.set(State.STOPPED);
		}
	}

	@Override
	public boolean isRunning() {
		return State.STARTED.equals(this.state.get());
	}

	/**
	 * Creates {@link RedisClient}.
	 *
	 * @return the {@link RedisClient} to use. Never {@literal null}.
	 */
	protected RedisClient createRedisClient() {
		var builder = RedisClient.builder().hostAndPort(getHostName(), getPort()).clientConfig(this.clientConfig);

		// Configure connection pool if pool configuration is provided
		clientConfiguration.getPoolConfig().ifPresent(poolConfig -> {
			builder.poolConfig(createConnectionPoolConfig(poolConfig));
		});

		return builder.build();
	}

	/**
	 * Creates {@link RedisSentinelClient}.
	 *
	 * @return the {@link RedisSentinelClient} to use. Never {@literal null}.
	 */
	@SuppressWarnings("NullAway")
	protected RedisSentinelClient createRedisSentinelClient() {

		RedisSentinelConfiguration config = getSentinelConfiguration();

		JedisClientConfig sentinelConfig = createSentinelClientConfig(config);

		var builder = RedisSentinelClient.builder()
				.masterName(config.getMaster() != null ? config.getMaster().getName() : null)
				.sentinels(convertToJedisSentinelSet(config.getSentinels())).clientConfig(this.clientConfig)
				.sentinelClientConfig(sentinelConfig);

		// Configure connection pool if pool configuration is provided
		clientConfiguration.getPoolConfig().ifPresent(poolConfig -> {
			builder.poolConfig(createConnectionPoolConfig(poolConfig));
		});

		return builder.build();
	}

	/**
	 * Creates {@link RedisClusterClient}.
	 *
	 * @return the {@link RedisClusterClient} to use. Never {@literal null}.
	 */
	@SuppressWarnings("NullAway")
	protected RedisClusterClient createRedisClusterClient() {

		RedisClusterConfiguration config = getClusterConfiguration();

		Set<HostAndPort> nodes = convertToJedisClusterSet(config.getClusterNodes());

		var builder = RedisClusterClient.builder().nodes(nodes).clientConfig(this.clientConfig);

		// Configure connection pool if pool configuration is provided
		clientConfiguration.getPoolConfig().ifPresent(poolConfig -> {
			builder.poolConfig(createConnectionPoolConfig(poolConfig));
		});

		return builder.build();
	}

	/**
	 * Creates a {@link ConnectionPoolConfig} from the provided {@link GenericObjectPoolConfig}. Maps all available Apache
	 * Commons Pool2 configuration options to Jedis ConnectionPoolConfig.
	 *
	 * @param poolConfig the pool configuration from Spring Data Redis
	 * @return the Jedis ConnectionPoolConfig with all options applied
	 */
	private ConnectionPoolConfig createConnectionPoolConfig(GenericObjectPoolConfig poolConfig) {
		ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();

		// Basic pool settings
		connectionPoolConfig.setMaxTotal(poolConfig.getMaxTotal());
		connectionPoolConfig.setMaxIdle(poolConfig.getMaxIdle());
		connectionPoolConfig.setMinIdle(poolConfig.getMinIdle());
		connectionPoolConfig.setBlockWhenExhausted(poolConfig.getBlockWhenExhausted());
		connectionPoolConfig.setMaxWait(poolConfig.getMaxWaitDuration());

		// Test settings
		connectionPoolConfig.setTestOnBorrow(poolConfig.getTestOnBorrow());
		connectionPoolConfig.setTestOnCreate(poolConfig.getTestOnCreate());
		connectionPoolConfig.setTestOnReturn(poolConfig.getTestOnReturn());
		connectionPoolConfig.setTestWhileIdle(poolConfig.getTestWhileIdle());

		// Eviction settings
		connectionPoolConfig.setTimeBetweenEvictionRuns(poolConfig.getDurationBetweenEvictionRuns());
		connectionPoolConfig.setNumTestsPerEvictionRun(poolConfig.getNumTestsPerEvictionRun());
		connectionPoolConfig.setMinEvictableIdleTime(poolConfig.getMinEvictableIdleDuration());
		connectionPoolConfig.setSoftMinEvictableIdleTime(poolConfig.getSoftMinEvictableIdleDuration());

		// Ordering and fairness
		connectionPoolConfig.setLifo(poolConfig.getLifo());
		connectionPoolConfig.setFairness(poolConfig.getFairness());

		// JMX and monitoring
		connectionPoolConfig.setJmxEnabled(poolConfig.getJmxEnabled());
		connectionPoolConfig.setJmxNamePrefix(poolConfig.getJmxNamePrefix());
		connectionPoolConfig.setJmxNameBase(poolConfig.getJmxNameBase());

		// Advanced settings
		connectionPoolConfig.setEvictionPolicyClassName(poolConfig.getEvictionPolicyClassName());
		connectionPoolConfig.setEvictorShutdownTimeout(poolConfig.getEvictorShutdownTimeoutDuration());

		return connectionPoolConfig;
	}

	@Override
	public void destroy() {

		stop();
		state.set(State.DESTROYED);
	}

	private void dispose(@Nullable RedisClient client) {
		if (client != null) {
			try {
				client.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Redis client", ex);
			}
		}
	}

	private void disposeSentinel(@Nullable RedisSentinelClient client) {
		if (client != null) {
			try {
				client.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Redis Sentinel client", ex);
			}
		}
	}

	private void disposeCluster(@Nullable RedisClusterClient client) {
		if (client != null) {
			try {
				client.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Redis Cluster client", ex);
			}
		}
	}

	@Override
	public RedisConnection getConnection() {
		assertInitialized();

		if (isRedisClusterAware()) {
			return getClusterConnection();
		}

		JedisClientConfig config = this.clientConfig;
		UnifiedJedis client;

		if (isRedisSentinelAware()) {
			SentinelConfiguration sentinelConfiguration = getSentinelConfiguration();

			if (sentinelConfiguration != null) {
				config = createSentinelClientConfig(sentinelConfiguration);
			}

			client = getRequiredSentinelClient();
		} else {
			client = getRequiredRedisClient();
		}

		JedisClientConnection connection = new JedisClientConnection(client, config);
		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);

		return postProcessConnection(connection);
	}

	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing initialization commands on a new
	 * connection. This implementation simply returns the connection.
	 *
	 * @param connection the jedis client connection.
	 * @return processed connection
	 */
	protected JedisClientConnection postProcessConnection(JedisClientConnection connection) {
		return connection;
	}

	@Override
	public RedisClusterConnection getClusterConnection() {

		assertInitialized();

		if (!isRedisClusterAware()) {
			throw new InvalidDataAccessResourceUsageException("Cluster is not configured");
		}

		return new JedisClientClusterConnection(getRequiredClusterClient());
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {

		assertInitialized();

		if (!isRedisSentinelAware()) {
			throw new InvalidDataAccessResourceUsageException("No Sentinels configured");
		}

		RedisSentinelConfiguration config = getSentinelConfiguration();

		if (config == null || config.getSentinels().isEmpty()) {
			throw new InvalidDataAccessResourceUsageException("No Sentinels configured");
		}

		// Get the first sentinel node and create a Jedis connection to it
		RedisNode sentinel = config.getSentinels().iterator().next();

		return new JedisSentinelConnection(sentinel);
	}

	@Override
	public @Nullable DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	@SuppressWarnings("NullAway")
	private RedisClient getRequiredRedisClient() {

		RedisClient client = this.redisClient;

		if (client == null) {
			throw new IllegalStateException("RedisClient is not initialized");
		}

		return client;
	}

	@SuppressWarnings("NullAway")
	private RedisSentinelClient getRequiredSentinelClient() {

		RedisSentinelClient client = this.sentinelClient;

		if (client == null) {
			throw new IllegalStateException("RedisSentinelClient is not initialized");
		}

		return client;
	}

	@SuppressWarnings("NullAway")
	private RedisClusterClient getRequiredClusterClient() {

		RedisClusterClient client = this.clusterClient;

		if (client == null) {
			throw new IllegalStateException("RedisClusterClient is not initialized");
		}

		return client;
	}

	@SuppressWarnings("NullAway")
	private void assertInitialized() {

		State current = state.get();

		if (State.STARTED.equals(current)) {
			return;
		}

		switch (current) {
			case CREATED, STOPPED -> throw new IllegalStateException(
					"JedisClientConnectionFactory has been %s. Use start() to initialize it".formatted(current));
			case DESTROYED -> throw new IllegalStateException(
					"JedisClientConnectionFactory was destroyed and cannot be used anymore");
			default -> throw new IllegalStateException("JedisClientConnectionFactory is %s".formatted(current));
		}
	}

	private int getReadTimeout() {
		return Math.toIntExact(clientConfiguration.getReadTimeout().toMillis());
	}

	private int getConnectTimeout() {
		return Math.toIntExact(clientConfiguration.getConnectTimeout().toMillis());
	}

	private MutableJedisClientConfiguration getMutableConfiguration() {

		Assert.state(clientConfiguration instanceof MutableJedisClientConfiguration,
				() -> "Client configuration must be instance of MutableJedisClientConfiguration but is %s"
						.formatted(ClassUtils.getShortName(clientConfiguration.getClass())));

		return (MutableJedisClientConfiguration) clientConfiguration;
	}

	/**
	 * Creates {@link JedisClientConfig} for Sentinel authentication.
	 *
	 * @param sentinelConfiguration the sentinel configuration
	 * @return the {@link JedisClientConfig} for sentinel authentication
	 */
	JedisClientConfig createSentinelClientConfig(SentinelConfiguration sentinelConfiguration) {
		return createClientConfig(0, sentinelConfiguration.getSentinelUsername(),
				sentinelConfiguration.getSentinelPassword());
	}

	/**
	 * Converts a collection of {@link RedisNode} to a set of {@link HostAndPort}.
	 *
	 * @param nodes the nodes to convert
	 * @return the converted set of {@link HostAndPort}
	 */
	private static Set<HostAndPort> convertToJedisSentinelSet(Collection<RedisNode> nodes) {

		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptySet();
		}

		Set<HostAndPort> convertedNodes = new LinkedHashSet<>(nodes.size());
		for (RedisNode node : nodes) {
			convertedNodes.add(JedisConverters.toHostAndPort(node));
		}
		return convertedNodes;
	}

	/**
	 * Converts a collection of {@link RedisNode} to a set of {@link HostAndPort} for cluster nodes.
	 *
	 * @param nodes the nodes to convert
	 * @return the converted set of {@link HostAndPort}
	 */
	private static Set<HostAndPort> convertToJedisClusterSet(Collection<RedisNode> nodes) {

		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptySet();
		}

		Set<HostAndPort> convertedNodes = new LinkedHashSet<>(nodes.size());
		for (RedisNode node : nodes) {
			convertedNodes.add(JedisConverters.toHostAndPort(node));
		}
		return convertedNodes;
	}

	/**
	 * Creates a {@link ClusterTopologyProvider} for the given {@link RedisClusterClient}.
	 *
	 * @param clusterClient the cluster client, must not be {@literal null}.
	 * @return the topology provider.
	 */
	protected ClusterTopologyProvider createTopologyProvider(RedisClusterClient clusterClient) {
		return new JedisClientClusterConnection.JedisClientClusterTopologyProvider(clusterClient);
	}

	/**
	 * Creates a {@link ClusterCommandExecutor} for the given {@link ClusterTopologyProvider}.
	 *
	 * @param topologyProvider the topology provider, must not be {@literal null}.
	 * @return the cluster command executor.
	 */
	protected ClusterCommandExecutor createClusterCommandExecutor(ClusterTopologyProvider topologyProvider) {
		return new ClusterCommandExecutor(topologyProvider,
				new JedisClientClusterConnection.JedisClientClusterNodeResourceProvider(this.clusterClient, topologyProvider),
				EXCEPTION_TRANSLATION, this.executor);
	}
}
