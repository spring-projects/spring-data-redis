/*
 * Copyright 2020-2022 the original author or authors.
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

import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

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

	private static final JedisClientConfiguration CLIENT_CONFIGURATION = JedisClientConfiguration.builder()
			.clientName("jedis-client").build();

	private static final NewableLazy<JedisConnectionFactory> STANDALONE = NewableLazy.of(() -> {

		ManagedJedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.standaloneConfiguration(),
				CLIENT_CONFIGURATION);

		factory.afterPropertiesSet();
		ShutdownQueue.register(factory.toCloseable());

		return factory;
	});

	private static final NewableLazy<JedisConnectionFactory> SENTINEL = NewableLazy.of(() -> {

		ManagedJedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.sentinelConfiguration(),
				CLIENT_CONFIGURATION);

		factory.afterPropertiesSet();
		ShutdownQueue.register(factory.toCloseable());

		return factory;
	});

	private static final NewableLazy<JedisConnectionFactory> CLUSTER = NewableLazy.of(() -> {

		ManagedJedisConnectionFactory factory = new ManagedJedisConnectionFactory(SettingsUtils.clusterConfiguration(),
				CLIENT_CONFIGURATION);

		factory.afterPropertiesSet();
		ShutdownQueue.register(factory.toCloseable());

		return factory;
	});

	private static final Map<Class<?>, NewableLazy<JedisConnectionFactory>> factories;

	static {

		factories = new HashMap<>();
		factories.put(RedisStanalone.class, STANDALONE);
		factories.put(RedisSentinel.class, SENTINEL);
		factories.put(RedisCluster.class, CLUSTER);
	}

	/**
	 * Obtain a cached {@link JedisConnectionFactory} described by {@code qualifier}. Instances are managed by this
	 * extension and will be shut down on JVM shutdown.
	 *
	 * @param qualifier an be any of {@link RedisStanalone}, {@link RedisSentinel}, {@link RedisCluster}.
	 * @return the managed {@link JedisConnectionFactory}.
	 */
	public static JedisConnectionFactory getConnectionFactory(Class<? extends Annotation> qualifier) {
		return factories.get(qualifier).getNew();
	}

	/**
	 * Obtain a new {@link JedisConnectionFactory} described by {@code qualifier}. Instances are managed by this extension
	 * and will be shut down on JVM shutdown.
	 *
	 * @param qualifier an be any of {@link RedisStanalone}, {@link RedisSentinel}, {@link RedisCluster}.
	 * @return the managed {@link JedisConnectionFactory}.
	 */
	public static JedisConnectionFactory getNewConnectionFactory(Class<? extends Annotation> qualifier) {
		return factories.get(qualifier).getNew();
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

	static class NewableLazy<T> {

		private final Lazy<? extends T> lazy;

		private NewableLazy(Supplier<? extends T> supplier) {
			this.lazy = Lazy.of(supplier);
		}

		public static <T> NewableLazy<T> of(Supplier<? extends T> supplier) {
			return new NewableLazy<>(supplier);
		}

		public T getNew() {
			return lazy.get();
		}
	}

	static class ManagedJedisConnectionFactory extends JedisConnectionFactory
			implements ConnectionFactoryTracker.Managed {

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
		public void destroy() {

			if (!mayClose) {
				throw new IllegalStateException(
						"Prematurely attempted to close ManagedJedisConnectionFactory. Shutdown hook didn't run yet which means that the test run isn't finished yet. Please fix the tests so that they don't close this connection factory.");
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

		Closeable toCloseable() {
			return () -> {
				try {
					mayClose = true;
					destroy();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}
	}
}
