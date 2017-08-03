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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Default implementation of {@literal LettucePoolingClientConfiguration}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class DefaultLettucePoolingClientConfiguration extends DefaultLettuceClientConfiguration
		implements LettucePoolingClientConfiguration {

	private final GenericObjectPoolConfig poolConfig;

	DefaultLettucePoolingClientConfiguration(boolean useSsl, boolean verifyPeer, boolean startTls,
			ClientResources clientResources, ClientOptions clientOptions, Duration timeout, Duration shutdownTimeout,
			GenericObjectPoolConfig poolConfig) {

		super(useSsl, verifyPeer, startTls, clientResources, clientOptions, timeout, shutdownTimeout);

		this.poolConfig = poolConfig;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration#isUsePooling()
	 */
	@Override
	public boolean isUsePooling() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration#getPoolConfig()
	 */
	@Override
	public GenericObjectPoolConfig getPoolConfig() {
		return poolConfig;
	}
}
