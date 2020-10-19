/*
 * Copyright 2020 the original author or authors.
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
import redis.clients.jedis.JedisShardInfo;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

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
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisSentinel;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.ShutdownQueue;
import org.springframework.data.util.Lazy;

/**
 * JUnit {@link ParameterResolver} providing pre-cached {@link JedisConnectionFactory} instances. Connection factories
 * can be qualified with {@code @RedisStanalone} (default), {@code @RedisSentinel} or {@code @RedisCluster} to obtain a
 * specific factory instance. Instances are managed by this extension and will be shut down on JVM shutdown.
 *
 * @author Mark Paluch
 * @see RedisStanalone
 * @see RedisSentinel
 * @see RedisCluster
 */
public class JedisConnectionFactoryExtension implements ParameterResolver {

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
			.create(JedisConnectionFactoryExtension.class);

	private static final Lazy<JedisConnectionFactory> STANDALONE = Lazy.of(() -> {

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().usePooling().build();

		JedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.standaloneConfiguration(),
				configuration);
		factory.afterPropertiesSet();
		ShutdownQueue.register(ShutdownQueue.toCloseable(factory));

		return factory;
	});

	private static final Lazy<JedisConnectionFactory> SENTINEL = Lazy.of(() -> {

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().usePooling().build();

		JedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.sentinelConfiguration(),
				configuration);
		factory.afterPropertiesSet();
		ShutdownQueue.register(ShutdownQueue.toCloseable(factory));

		return factory;
	});

	private static final Lazy<JedisConnectionFactory> CLUSTER = Lazy.of(() -> {

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().usePooling().build();

		JedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.clusterConfiguration(),
				configuration);
		factory.afterPropertiesSet();
		ShutdownQueue.register(ShutdownQueue.toCloseable(factory));

		return factory;
	});

	private static final Map<Class<?>, Lazy<JedisConnectionFactory>> factories;

	static {

		factories = new HashMap<>();
		factories.put(RedisStanalone.class, STANDALONE);
		factories.put(RedisSentinel.class, SENTINEL);
		factories.put(RedisCluster.class, CLUSTER);
	}

	/**
	 * Obtain a {@link JedisConnectionFactory} described by {@code qualifier}. Instances are managed by this extension and
	 * will be shut down on JVM shutdown.
	 *
	 * @param qualifier an be any of {@link RedisStanalone}, {@link RedisSentinel}, {@link RedisCluster}.
	 * @return the managed {@link JedisConnectionFactory}.
	 */
	public static JedisConnectionFactory getConnectionFactory(Class<? extends Annotation> qualifier) {
		return factories.get(qualifier).get();
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return RedisConnectionFactory.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);

		Class<? extends Annotation> qualifier = getQualifier(parameterContext);

		return store.getOrComputeIfAbsent(qualifier, JedisConnectionFactoryExtension::getConnectionFactory);
	}

	private static Class<? extends Annotation> getQualifier(ParameterContext parameterContext) {

		if (parameterContext.isAnnotated(RedisSentinel.class)) {
			return RedisSentinel.class;
		}

		if (parameterContext.isAnnotated(RedisCluster.class)) {
			return RedisCluster.class;
		}

		return RedisStanalone.class;
	}

	static class ManagedJedisConnectionFactory extends JedisConnectionFactory
			implements ConnectionFactoryTracker.Managed {

		public ManagedJedisConnectionFactory() {
			super();
		}

		public ManagedJedisConnectionFactory(JedisShardInfo shardInfo) {
			super(shardInfo);
		}

		public ManagedJedisConnectionFactory(JedisPoolConfig poolConfig) {
			super(poolConfig);
		}

		public ManagedJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig) {
			super(sentinelConfig);
		}

		public ManagedJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisPoolConfig poolConfig) {
			super(sentinelConfig, poolConfig);
		}

		public ManagedJedisConnectionFactory(RedisClusterConfiguration clusterConfig) {
			super(clusterConfig);
		}

		public ManagedJedisConnectionFactory(RedisClusterConfiguration clusterConfig, JedisPoolConfig poolConfig) {
			super(clusterConfig, poolConfig);
		}

		public ManagedJedisConnectionFactory(RedisStandaloneConfiguration standaloneConfig) {
			super(standaloneConfig);
		}

		public ManagedJedisConnectionFactory(RedisStandaloneConfiguration standaloneConfig,
				JedisClientConfiguration clientConfig) {
			super(standaloneConfig, clientConfig);
		}

		public ManagedJedisConnectionFactory(RedisSentinelConfiguration sentinelConfig,
				JedisClientConfiguration clientConfig) {
			super(sentinelConfig, clientConfig);
		}

		public ManagedJedisConnectionFactory(RedisClusterConfiguration clusterConfig,
				JedisClientConfiguration clientConfig) {
			super(clusterConfig, clientConfig);
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
	}
}
