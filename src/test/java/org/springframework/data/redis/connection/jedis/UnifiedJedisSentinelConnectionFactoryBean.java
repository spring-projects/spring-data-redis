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
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;

/**
 * Factory bean that creates a {@link JedisConnectionFactory} configured to use
 * the modern Jedis 7.x API with {@link RedisSentinelClient} for sentinel deployments.
 * <p>
 * This is primarily used for XML-based Spring configuration in tests.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
public class UnifiedJedisSentinelConnectionFactoryBean implements FactoryBean<JedisConnectionFactory>, InitializingBean {

	private int timeout = 60000;
	private String clientName = "unified-jedis-sentinel-client";

	private JedisConnectionFactory connectionFactory;

	@Override
	public void afterPropertiesSet() {
		RedisSentinelConfiguration sentinelConfig = SettingsUtils.sentinelConfiguration();

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder()
				.clientName(clientName)
				.readTimeout(Duration.ofMillis(timeout))
				.connectTimeout(Duration.ofMillis(timeout))
				.build();

		connectionFactory = new JedisConnectionFactory(sentinelConfig, clientConfig);
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

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
}

