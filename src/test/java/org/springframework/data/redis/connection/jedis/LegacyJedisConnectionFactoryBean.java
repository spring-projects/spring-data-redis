/*
 * Copyright 2025-present the original author or authors.
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

import java.time.Duration;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Factory bean that creates a {@link JedisConnectionFactory} configured to use
 * the legacy {@link JedisConnection} API instead of the modern {@link UnifiedJedisConnection}.
 * <p>
 * This is primarily used for XML-based Spring configuration in tests to ensure
 * the legacy code path is exercised even when Jedis 7.3+ is on the classpath.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
public class LegacyJedisConnectionFactoryBean implements FactoryBean<JedisConnectionFactory>, InitializingBean {

	private String hostName = "localhost";
	private int port = 6379;
	private int timeout = 2000;
	private String clientName;
	private boolean usePool = false;

	private JedisConnectionFactory connectionFactory;

	@Override
	public void afterPropertiesSet() {
		RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(hostName, port);

		JedisClientConfiguration.JedisClientConfigurationBuilder builder = JedisClientConfiguration.builder()
				.clientName(clientName)
				.readTimeout(Duration.ofMillis(timeout))
				.connectTimeout(Duration.ofMillis(timeout));

		// Configure pooling based on usePool flag
		if (usePool) {
			builder.usePooling();
		}

		JedisClientConfiguration clientConfig = builder.build();

		// Create a subclass that forces legacy mode
		connectionFactory = new JedisConnectionFactory(standaloneConfig, clientConfig) {
			@Override
			public boolean isUsingUnifiedJedisConnection() {
				return false; // Force legacy JedisConnection
			}
		};
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();
	}

	@Override
	public JedisConnectionFactory getObject() {
		return connectionFactory;
	}

	@Override
	public Class<?> getObjectType() {
		return JedisConnectionFactory.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public void setUsePool(boolean usePool) {
		this.usePool = usePool;
	}
}

