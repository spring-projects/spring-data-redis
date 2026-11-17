/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis.extension;

import redis.clients.jedis.JedisPoolConfig;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.test.RedisTestExtensionSupport;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisSentinel;
import org.springframework.data.redis.test.extension.RedisStandalone;
import org.springframework.data.redis.test.extension.ShutdownQueue;
import org.springframework.data.util.Lazy;

/**
 * JUnit {@link ParameterResolver} providing pre-cached {@link JedisConnectionFactory} instances. Connection factories
 * can be qualified with {@code @RedisStanalone} (default), {@code @RedisSentinel} or {@code @RedisCluster} to obtain a
 * specific factory instance. Instances are managed by this extension and will be shut down on JVM shutdown.
 *
 * @author Mark Paluch
 * @see RedisStandalone
 * @see RedisSentinel
 * @see RedisCluster
 */
public class JedisConnectionFactoryExtension extends RedisTestExtensionSupport implements ParameterResolver {

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
			.create(JedisConnectionFactoryExtension.class);

	private static final JedisClientConfiguration CLIENT_CONFIGURATION = JedisClientConfiguration.builder()
			.clientName("jedis-client").build();

	private static final JedisClientConfiguration POOLED_CLIENT_CONFIGURATION = JedisClientConfiguration.builder()
			.clientName("jedis-client").usePooling().poolConfig(poolConfig()).build();

	private static final Map<Class<?>, Lazy<JedisConnectionFactory>> pooledFactories = Map.of( //
			RedisStandalone.class, pooled(SettingsUtils.standaloneConfiguration(), ManagedJedisConnectionFactory::new),
			RedisSentinel.class, pooled(SettingsUtils.sentinelConfiguration(), ManagedJedisConnectionFactory::new),
			RedisCluster.class, pooled(SettingsUtils.clusterConfiguration(), ManagedJedisConnectionFactory::new));

	private static final Map<Class<?>, Lazy<JedisConnectionFactory>> unpooledFactories = Map.of( //
			RedisStandalone.class, unpooled(SettingsUtils.standaloneConfiguration(), ManagedJedisConnectionFactory::new),
			RedisSentinel.class, unpooled(SettingsUtils.sentinelConfiguration(), ManagedJedisConnectionFactory::new),
			RedisCluster.class, unpooled(SettingsUtils.clusterConfiguration(), ManagedJedisConnectionFactory::new));

	private static <T> Lazy<JedisConnectionFactory> pooled(T configuration,
			BiFunction<T, JedisClientConfiguration, ManagedJedisConnectionFactory> mapper) {
		return lazyOf(configuration, POOLED_CLIENT_CONFIGURATION, mapper);
	}

	private static <T> Lazy<JedisConnectionFactory> unpooled(T configuration,
			BiFunction<T, JedisClientConfiguration, ManagedJedisConnectionFactory> mapper) {
		return lazyOf(configuration, CLIENT_CONFIGURATION, mapper);
	}

	private static <T> Lazy<JedisConnectionFactory> lazyOf(T configuration, JedisClientConfiguration clientConfiguration,
			BiFunction<T, JedisClientConfiguration, ManagedJedisConnectionFactory> mapper) {

		return Lazy.of(() -> {

			ManagedJedisConnectionFactory factory = mapper.apply(configuration, clientConfiguration);

			factory.afterPropertiesSet();
			factory.start();
			ShutdownQueue.register(factory);

			return factory;
		});
	}

	/**
	 * Obtain a pooled {@link JedisConnectionFactory} described by {@code qualifier}. Instances are managed by this
	 * extension and will be shut down on JVM shutdown.
	 *
	 * @param qualifier can be any of {@link RedisStandalone}, {@link RedisSentinel}, {@link RedisCluster}.
	 * @return the managed {@link JedisConnectionFactory}.
	 */
	public static JedisConnectionFactory getConnectionFactory(Class<? extends Annotation> qualifier) {
		return getConnectionFactory(qualifier, true);
	}

	/**
	 * Obtain a {@link JedisConnectionFactory} described by {@code qualifier}. Instances are managed by this extension and
	 * will be shut down on JVM shutdown.
	 *
	 * @param qualifier can be any of {@link RedisStandalone}, {@link RedisSentinel}, {@link RedisCluster}.
	 * @return the managed {@link JedisConnectionFactory}.
	 */
	public static JedisConnectionFactory getConnectionFactory(Class<? extends Annotation> qualifier, boolean pooled) {
		return (pooled ? pooledFactories : unpooledFactories).get(qualifier).get();
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return RedisConnectionFactory.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = getSessionStore(extensionContext, NAMESPACE);

		Class<? extends Annotation> qualifier = getQualifier(parameterContext);

		return store.getOrComputeIfAbsent(qualifier, JedisConnectionFactoryExtension::getConnectionFactory);
	}

	/**
	 * Validate idle connections in the background as tests may leave pooled connections broken. FIFO handles rotation of
	 * connections.
	 */
	private static JedisPoolConfig poolConfig() {

		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setLifo(false);
		poolConfig.setMaxTotal(8);
		poolConfig.setMaxWait(Duration.ofSeconds(5));
		poolConfig.setMinIdle(4);
		poolConfig.setTestWhileIdle(true);
		poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(50));
		poolConfig.setJmxEnabled(false);

		return poolConfig;
	}

	private static Class<? extends Annotation> getQualifier(ParameterContext parameterContext) {

		if (parameterContext.isAnnotated(RedisSentinel.class)) {
			return RedisSentinel.class;
		}

		if (parameterContext.isAnnotated(RedisCluster.class)) {
			return RedisCluster.class;
		}

		return RedisStandalone.class;
	}

	/**
	 * Managed connection factory that forces legacy {@link org.springframework.data.redis.connection.jedis.JedisConnection}
	 * mode for testing the legacy code path.
	 */
	static class ManagedJedisConnectionFactory extends JedisConnectionFactory
			implements ConnectionFactoryTracker.Managed, ShutdownQueue.ShutdownCloseable {

		private volatile boolean mayClose;

		ManagedJedisConnectionFactory(RedisStandaloneConfiguration standaloneConfig,
				JedisClientConfiguration clientConfig) {
			super(standaloneConfig, clientConfig);
		}

		ManagedJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisClientConfiguration clientConfig) {
			super(sentinelConfig, clientConfig);
		}

		ManagedJedisConnectionFactory(RedisClusterConfiguration clusterConfig, JedisClientConfiguration clientConfig) {
			super(clusterConfig, clientConfig);
		}

		@Override
		public boolean isUseUnifiedJedis() {
			return false; // Force legacy JedisConnection for testing
		}

		@Override
		public void destroy() {

			if (!mayClose) {
				throw new IllegalStateException(
						"Prematurely attempted to close ManagedJedisConnectionFactory; Shutdown hook didn't run yet which means that the test run isn't finished yet; Please fix the tests so that they don't close this connection factory.");
			}

			super.destroy();
		}

		@Override
		public String toString() {

			StringBuilder builder = new StringBuilder("Jedis");

			if (isRedisClusterAware()) {
				builder.append(" Cluster");
			}

			if (isRedisSentinelAware()) {
				builder.append(" Sentinel");
			}

			if (getUsePool()) {
				builder.append(" [pool]");
			}

			return builder.toString();
		}

		@Override
		public void close() {
			try {
				mayClose = true;
				destroy();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
