/*
 * Copyright 2017 the original author or authors.
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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Default implementation of {@literal LettucePoolingClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isUseSsl()
	 */
	@Override
	public boolean isUseSsl() {
		return clientConfiguration.isUseSsl();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isVerifyPeer()
	 */
	@Override
	public boolean isVerifyPeer() {
		return clientConfiguration.isVerifyPeer();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isStartTls()
	 */
	@Override
	public boolean isStartTls() {
		return clientConfiguration.isStartTls();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientResources()
	 */
	@Override
	public Optional<ClientResources> getClientResources() {
		return clientConfiguration.getClientResources();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientOptions()
	 */
	@Override
	public Optional<ClientOptions> getClientOptions() {
		return clientConfiguration.getClientOptions();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getClientName()
	 */
	@Override
	public Optional<String> getClientName() {
		return clientConfiguration.getClientName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getCommandTimeout()
	 */
	@Override
	public Duration getCommandTimeout() {
		return clientConfiguration.getCommandTimeout();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#getShutdownTimeout()
	 */
	@Override
	public Duration getShutdownTimeout() {
		return clientConfiguration.getShutdownTimeout();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettucePoolingClientConfiguration#getPoolConfig()
	 */
	@Override
	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}
}
