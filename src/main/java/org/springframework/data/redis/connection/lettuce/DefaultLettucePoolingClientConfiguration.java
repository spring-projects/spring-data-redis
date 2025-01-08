/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Default implementation of {@literal LettucePoolingClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Yanming Zhou
 * @since 2.0
 */
class DefaultLettucePoolingClientConfiguration implements LettucePoolingClientConfiguration {

	private final LettuceClientConfiguration clientConfiguration;
	private final GenericObjectPoolConfig poolConfig;

	DefaultLettucePoolingClientConfiguration(LettuceClientConfiguration clientConfiguration,
			GenericObjectPoolConfig poolConfig) {

		this.clientConfiguration = clientConfiguration;
		this.poolConfig = poolConfig;
	}

	@Override
	public boolean isUseSsl() {
		return clientConfiguration.isUseSsl();
	}

	@Override
	public boolean isVerifyPeer() {
		return clientConfiguration.isVerifyPeer();
	}

	@Override
	public boolean isStartTls() {
		return clientConfiguration.isStartTls();
	}

	@Override
	public Optional<ClientResources> getClientResources() {
		return clientConfiguration.getClientResources();
	}

	@Override
	public Optional<ClientOptions> getClientOptions() {
		return clientConfiguration.getClientOptions();
	}

	@Override
	public Optional<String> getClientName() {
		return clientConfiguration.getClientName();
	}

	@Override
	public Optional<ReadFrom> getReadFrom() {
		return clientConfiguration.getReadFrom();
	}

	@Override
	public Optional<RedisCredentialsProviderFactory> getRedisCredentialsProviderFactory() {
		return clientConfiguration.getRedisCredentialsProviderFactory();
	}

	@Override
	public Duration getCommandTimeout() {
		return clientConfiguration.getCommandTimeout();
	}

	@Override
	public Duration getShutdownTimeout() {
		return clientConfiguration.getShutdownTimeout();
	}

	@Override
	public Duration getShutdownQuietPeriod() {
		return clientConfiguration.getShutdownQuietPeriod();
	}

	@Override
	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}
}
