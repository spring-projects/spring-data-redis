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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
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
import org.springframework.util.StringUtils;

/**
 * Connection factory creating <a href="http://github.com/xetorthio/jedis">Jedis</a> based connections.
 * <p>
 * {@link JedisConnectionFactory} should be configured using an environmental configuration and the
 * {@link JedisClientConfiguration client configuration}. Jedis supports the following environmental configurations:
 * <ul>
 * <li>{@link RedisStandaloneConfiguration}</li>
 * <li>{@link RedisSentinelConfiguration}</li>
 * <li>{@link RedisClusterConfiguration}</li>
 * </ul>
 *
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Fu Jian
 * @see JedisClientConfiguration
 * @see Jedis
 */
public class JedisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private final static Log log = LogFactory.getLog(JedisConnectionFactory.class);
	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private final JedisClientConfiguration clientConfiguration;
	private @Nullable JedisShardInfo shardInfo;
	private boolean providedShardInfo = false;
	private @Nullable Pool<Jedis> pool;
	private boolean convertPipelineAndTxResults = true;
	private RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration("localhost",
			Protocol.DEFAULT_PORT);

	private @Nullable RedisConfiguration configuration;

	private @Nullable JedisCluster cluster;
	private @Nullable ClusterCommandExecutor clusterCommandExecutor;

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance with default settings (default connection pooling, no
	 * shard information).
	 */
	public JedisConnectionFactory() {
		this(new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance given {@link JedisClientConfiguration}.
	 *
	 * @param clientConfig must not be {@literal null}
	 * @since 2.0
	 */
	private JedisConnectionFactory(JedisClientConfiguration clientConfig) {

		Assert.notNull(clientConfig, "JedisClientConfiguration must not be null!");

		this.clientConfiguration = clientConfig;
	}

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance. Will override the other connection parameters passed
	 * to the factory.
	 *
	 * @param shardInfo shard information
	 * @deprecated since 2.0, configure Jedis with {@link JedisClientConfiguration} and
	 *             {@link RedisStandaloneConfiguration}.
	 */
	@Deprecated
	public JedisConnectionFactory(JedisShardInfo shardInfo) {

		this(MutableJedisClientConfiguration.create(shardInfo));

		this.shardInfo = shardInfo;
		this.providedShardInfo = true;
	}

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance using the given pool configuration.
	 *
	 * @param poolConfig pool configuration
	 */
	public JedisConnectionFactory(JedisPoolConfig poolConfig) {
		this((RedisSentinelConfiguration) null, poolConfig);
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 *
	 * @param sentinelConfig must not be {@literal null}.
	 * @since 1.4
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfig) {
		this(sentinelConfig, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link JedisPoolConfig} applied to
	 * {@link JedisSentinelPool}.
	 *
	 * @param sentinelConfig the sentinel configuration to use.
	 * @param poolConfig pool configuration. Defaulted to new instance if {@literal null}.
	 * @since 1.4
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisPoolConfig poolConfig) {

		this.configuration = sentinelConfig;
		this.clientConfiguration = MutableJedisClientConfiguration
				.create(poolConfig != null ? poolConfig : new JedisPoolConfig());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 *
	 * @param clusterConfig must not be {@literal null}.
	 * @since 1.7
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfig) {
		this(clusterConfig, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} applied
	 * to create a {@link JedisCluster}.
	 *
	 * @param clusterConfig must not be {@literal null}.
	 * @since 1.7
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfig, JedisPoolConfig poolConfig) {

		Assert.notNull(clusterConfig, "RedisClusterConfiguration must not be null!");

		this.configuration = clusterConfig;
		this.clientConfiguration = MutableJedisClientConfiguration.create(poolConfig);
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisStandaloneConfiguration}.
	 *
	 * @param standaloneConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisStandaloneConfiguration standaloneConfig) {
		this(standaloneConfig, new MutableJedisClientConfiguration());
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisStandaloneConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param standaloneConfig must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisStandaloneConfiguration standaloneConfig, JedisClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(standaloneConfig, "RedisStandaloneConfiguration must not be null!");

		this.standaloneConfig = standaloneConfig;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisSentinelConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param sentinelConfig must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(sentinelConfig, "RedisSentinelConfiguration must not be null!");

		this.configuration = sentinelConfig;
	}

	/**
	 * Constructs a new {@link JedisConnectionFactory} instance using the given {@link RedisClusterConfiguration} and
	 * {@link JedisClientConfiguration}.
	 *
	 * @param clusterConfig must not be {@literal null}.
	 * @param clientConfig must not be {@literal null}.
	 * @since 2.0
	 */
	public JedisConnectionFactory(RedisClusterConfiguration clusterConfig, JedisClientConfiguration clientConfig) {

		this(clientConfig);

		Assert.notNull(clusterConfig, "RedisClusterConfiguration must not be null!");

		this.configuration = clusterConfig;
	}

	/**
	 * Returns a Jedis instance to be used as a Redis connection. The instance can be newly created or retrieved from a
	 * pool.
	 *
	 * @return Jedis instance ready for wrapping into a {@link RedisConnection}.
	 */
	protected Jedis fetchJedisConnector() {
		try {

			if (getUsePool() && pool != null) {
				return pool.getResource();
			}

			Jedis jedis = createJedis();
			// force initialization (see Jedis issue #82)
			jedis.connect();

			potentiallySetClientName(jedis);
			return jedis;
		} catch (Exception ex) {
			throw new RedisConnectionFailureException("Cannot get Jedis connection", ex);
		}
	}

	private Jedis createJedis() {

		if (providedShardInfo) {
			return new Jedis(getShardInfo());
		}

		Jedis jedis = new Jedis(getHostName(), getPort(), getConnectTimeout(), getReadTimeout(), isUseSsl(),
				clientConfiguration.getSslSocketFactory().orElse(null), //
				clientConfiguration.getSslParameters().orElse(null), //
				clientConfiguration.getHostnameVerifier().orElse(null));

		Client client = jedis.getClient();

		getRedisPassword().map(String::new).ifPresent(client::setPassword);
		client.setDb(getDatabase());

		return jedis;
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {

		if (shardInfo == null && clientConfiguration instanceof MutableJedisClientConfiguration) {

			providedShardInfo = false;
			shardInfo = new JedisShardInfo(getHostName(), getPort(), isUseSsl(), //
					clientConfiguration.getSslSocketFactory().orElse(null), //
					clientConfiguration.getSslParameters().orElse(null), //
					clientConfiguration.getHostnameVerifier().orElse(null));

			getRedisPassword().map(String::new).ifPresent(shardInfo::setPassword);

			int readTimeout = getReadTimeout();

			if (readTimeout > 0) {
				shardInfo.setSoTimeout(readTimeout);
			}

			getMutableConfiguration().setShardInfo(shardInfo);
		}

		if (getUsePool() && !isRedisClusterAware()) {
			this.pool = createPool();
		}

		if (isRedisClusterAware()) {
			this.cluster = createCluster();
		}
	}

	private Pool<Jedis> createPool() {

		if (isRedisSentinelAware()) {
			return createRedisSentinelPool((RedisSentinelConfiguration) this.configuration);
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

		GenericObjectPoolConfig poolConfig = getPoolConfig() != null ? getPoolConfig() : new JedisPoolConfig();
		return new JedisSentinelPool(config.getMaster().getName(), convertToJedisSentinelSet(config.getSentinels()),
				poolConfig, getConnectTimeout(), getReadTimeout(), getPassword(), getDatabase(), getClientName());
	}

	/**
	 * Creates {@link JedisPool}.
	 *
	 * @return the {@link Pool} to use. Never {@literal null}.
	 * @since 1.4
	 */
	protected Pool<Jedis> createRedisPool() {

		return new JedisPool(getPoolConfig(), getHostName(), getPort(), getConnectTimeout(), getReadTimeout(),
				getPassword(), getDatabase(), getClientName(), isUseSsl(),
				clientConfiguration.getSslSocketFactory().orElse(null), //
				clientConfiguration.getSslParameters().orElse(null), //
				clientConfiguration.getHostnameVerifier().orElse(null));
	}

	private JedisCluster createCluster() {

		JedisCluster cluster = createCluster((RedisClusterConfiguration) this.configuration, getPoolConfig());
		JedisClusterTopologyProvider topologyProvider = new JedisClusterTopologyProvider(cluster);
		this.clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider,
				new JedisClusterConnection.JedisClusterNodeResourceProvider(cluster, topologyProvider), EXCEPTION_TRANSLATION);
		return cluster;
	}

	/**
	 * Creates {@link JedisCluster} for given {@link RedisClusterConfiguration} and {@link GenericObjectPoolConfig}.
	 *
	 * @param clusterConfig must not be {@literal null}.
	 * @param poolConfig can be {@literal null}.
	 * @return the actual {@link JedisCluster}.
	 * @since 1.7
	 */
	protected JedisCluster createCluster(RedisClusterConfiguration clusterConfig, GenericObjectPoolConfig poolConfig) {

		Assert.notNull(clusterConfig, "Cluster configuration must not be null!");

		Set<HostAndPort> hostAndPort = new HashSet<>();
		for (RedisNode node : clusterConfig.getClusterNodes()) {
			hostAndPort.add(new HostAndPort(node.getHost(), node.getPort()));
		}

		int redirects = clusterConfig.getMaxRedirects() != null ? clusterConfig.getMaxRedirects() : 5;

		int connectTimeout = getConnectTimeout();
		int readTimeout = getReadTimeout();

		return StringUtils.hasText(getPassword())
				? new JedisCluster(hostAndPort, connectTimeout, readTimeout, redirects, getPassword(), poolConfig)
				: new JedisCluster(hostAndPort, connectTimeout, readTimeout, redirects, poolConfig);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() {

		if (getUsePool() && pool != null) {

			try {
				pool.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close Jedis pool", ex);
			}
			pool = null;
		}

		if (cluster != null) {

			try {
				cluster.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Jedis cluster", ex);
			}

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

		if (isRedisClusterAware()) {
			return getClusterConnection();
		}

		Jedis jedis = fetchJedisConnector();
		String clientName = clientConfiguration.getClientName().orElse(null);
		JedisConnection connection = (getUsePool() ? new JedisConnection(jedis, pool, getDatabase(), clientName)
				: new JedisConnection(jedis, null, getDatabase(), clientName));
		connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);
		return postProcessConnection(connection);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getClusterConnection()
	 */
	@Override
	public RedisClusterConnection getClusterConnection() {

		if (!isRedisClusterAware()) {
			throw new InvalidDataAccessApiUsageException("Cluster is not configured!");
		}
		return new JedisClusterConnection(cluster, clusterCommandExecutor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
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
	public String getPassword() {
		return getRedisPassword().map(String::new).orElse(null);
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

		if (RedisConfiguration.isPasswordAware(configuration)) {

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
	 * Returns the shardInfo.
	 *
	 * @return the shardInfo.
	 * @deprecated since 2.0.
	 */
	@Deprecated
	@Nullable
	public JedisShardInfo getShardInfo() {
		return shardInfo;
	}

	/**
	 * Sets the shard info for this factory.
	 *
	 * @param shardInfo the shardInfo to set.
	 * @deprecated since 2.0, configure the individual properties from {@link JedisShardInfo} using
	 *             {@link JedisClientConfiguration}.
	 * @throws IllegalStateException if {@link JedisClientConfiguration} is immutable.
	 */
	@Deprecated
	public void setShardInfo(JedisShardInfo shardInfo) {

		this.shardInfo = shardInfo;
		this.providedShardInfo = true;
		getMutableConfiguration().setShardInfo(shardInfo);
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
	 *
	 * @return the use of connection pooling.
	 */
	public boolean getUsePool() {

		// Jedis Sentinel cannot operate without a pool.
		if (isRedisSentinelAware()) {
			return true;
		}

		return clientConfiguration.isUsePooling();
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
			throw new IllegalStateException("Jedis requires pooling for Redis Sentinel use!");
		}

		getMutableConfiguration().setUsePooling(usePool);
	}

	/**
	 * Returns the poolConfig.
	 *
	 * @return the poolConfig
	 */
	@Nullable
	public GenericObjectPoolConfig getPoolConfig() {
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
	 * @deprecated since 2.0, configure the client name using {@link RedisSentinelConfiguration} or
	 *             {@link RedisStandaloneConfiguration}.
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
		return clientConfiguration;
	}

	/**
	 * @return the {@link RedisStandaloneConfiguration}.
	 * @since 2.0
	 */
	@Nullable
	public RedisStandaloneConfiguration getStandaloneConfiguration() {
		return standaloneConfig;
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

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
	 * Jedis driver.
	 *
	 * @return Whether or not to convert pipeline and tx results.
	 */
	public boolean getConvertPipelineAndTxResults() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link JedisConnection#closePipeline()} and {@link JedisConnection#exec()} will be of the type returned by the
	 * Jedis driver.
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results.
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

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory#getSentinelConnection()
	 */
	@Override
	public RedisSentinelConnection getSentinelConnection() {

		if (!isRedisSentinelAware()) {
			throw new InvalidDataAccessResourceUsageException("No Sentinels configured");
		}

		return new JedisSentinelConnection(getActiveSentinel());
	}

	private Jedis getActiveSentinel() {

		Assert.isTrue(RedisConfiguration.isSentinelConfiguration(configuration), "SentinelConfig must not be null!");

		for (RedisNode node : ((SentinelConfiguration) configuration).getSentinels()) {

			Jedis jedis = new Jedis(node.getHost(), node.getPort(), getConnectTimeout(), getReadTimeout());

			if (jedis.ping().equalsIgnoreCase("pong")) {

				potentiallySetClientName(jedis);
				return jedis;
			}
		}

		throw new InvalidDataAccessResourceUsageException("No Sentinel found");
	}

	private Set<String> convertToJedisSentinelSet(Collection<RedisNode> nodes) {

		if (CollectionUtils.isEmpty(nodes)) {
			return Collections.emptySet();
		}

		Set<String> convertedNodes = new LinkedHashSet<>(nodes.size());
		for (RedisNode node : nodes) {
			if (node != null) {
				convertedNodes.add(node.asString());
			}
		}
		return convertedNodes;
	}

	private void potentiallySetClientName(Jedis jedis) {
		clientConfiguration.getClientName().ifPresent(jedis::clientSetname);
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

		public static JedisClientConfiguration create(JedisShardInfo shardInfo) {

			MutableJedisClientConfiguration configuration = new MutableJedisClientConfiguration();
			configuration.setShardInfo(shardInfo);
			return configuration;
		}

		public static JedisClientConfiguration create(GenericObjectPoolConfig jedisPoolConfig) {

			MutableJedisClientConfiguration configuration = new MutableJedisClientConfiguration();
			configuration.setPoolConfig(jedisPoolConfig);
			return configuration;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#isUseSsl()
		 */
		@Override
		public boolean isUseSsl() {
			return useSsl;
		}

		public void setUseSsl(boolean useSsl) {
			this.useSsl = useSsl;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getSslSocketFactory()
		 */
		@Override
		public Optional<SSLSocketFactory> getSslSocketFactory() {
			return Optional.ofNullable(sslSocketFactory);
		}

		public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
			this.sslSocketFactory = sslSocketFactory;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getSslParameters()
		 */
		@Override
		public Optional<SSLParameters> getSslParameters() {
			return Optional.ofNullable(sslParameters);
		}

		public void setSslParameters(SSLParameters sslParameters) {
			this.sslParameters = sslParameters;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getHostnameVerifier()
		 */
		@Override
		public Optional<HostnameVerifier> getHostnameVerifier() {
			return Optional.ofNullable(hostnameVerifier);
		}

		public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
			this.hostnameVerifier = hostnameVerifier;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#isUsePooling()
		 */
		@Override
		public boolean isUsePooling() {
			return usePooling;
		}

		public void setUsePooling(boolean usePooling) {
			this.usePooling = usePooling;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getPoolConfig()
		 */
		@Override
		public Optional<GenericObjectPoolConfig> getPoolConfig() {
			return Optional.ofNullable(poolConfig);
		}

		public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
			this.poolConfig = poolConfig;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getClientName()
		 */
		@Override
		public Optional<String> getClientName() {
			return Optional.ofNullable(clientName);
		}

		public void setClientName(String clientName) {
			this.clientName = clientName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getReadTimeout()
		 */
		@Override
		public Duration getReadTimeout() {
			return readTimeout;
		}

		public void setReadTimeout(Duration readTimeout) {
			this.readTimeout = readTimeout;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getConnectTimeout()
		 */
		@Override
		public Duration getConnectTimeout() {
			return connectTimeout;
		}

		public void setConnectTimeout(Duration connectTimeout) {
			this.connectTimeout = connectTimeout;
		}

		public void setShardInfo(JedisShardInfo shardInfo) {

			setSslSocketFactory(shardInfo.getSslSocketFactory());
			setSslParameters(shardInfo.getSslParameters());
			setHostnameVerifier(shardInfo.getHostnameVerifier());
			setUseSsl(shardInfo.getSsl());
			setConnectTimeout(Duration.ofMillis(shardInfo.getConnectionTimeout()));
			setReadTimeout(Duration.ofMillis(shardInfo.getSoTimeout()));
		}
	}
}
