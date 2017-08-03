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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public interface LettucePoolingClientConfiguration extends LettuceClientConfiguration {

	/**
	 * Creates a new {@link LettucePoolingClientConfigurationBuilder} to build {@link LettucePoolingClientConfiguration}
	 * to be used with the jedis client.
	 *
	 * @return a new {@link LettucePoolingClientConfigurationBuilder} to build {@link LettucePoolingClientConfiguration}.
	 */
	static LettucePoolingClientConfigurationBuilder builder() {
		return new DefaultLettucePoolingClientConfigurationBuilder();
	}

	/**
	 * Creates a default {@link LettucePoolingClientConfiguration} with:
	 * <dl>
	 * <dt>SSL</dt>
	 * <dd>no</dd>
	 * <dt>Pooling</dt>
	 * <dd>yes</dd>
	 * <dt>Peer Verification</dt>
	 * <dd>yes</dd>
	 * <dt>Start TLS</dt>
	 * <dd>no</dd>
	 * <dt>Client Options</dt>
	 * <dd>none</dd>
	 * <dt>Client Resources</dt>
	 * <dd>none</dd>
	 * <dt>Connect Timeout</dt>
	 * <dd>60 Seconds</dd>
	 * <dt>Shutdown Timeout</dt>
	 * <dd>2 Seconds</dd>
	 * </dl>
	 *
	 * @return a {@link LettucePoolingClientConfiguration} with defaults.
	 */
	static LettucePoolingClientConfiguration defaultConfiguration() {
		return builder().build();
	}

	/**
	 * @return the optional {@link GenericObjectPoolConfig}.
	 */
	GenericObjectPoolConfig getPoolConfig();

	/**
	 * Builder for Pooling-related {@link LettuceClientConfiguration}.
	 */
	interface LettucePoolingClientConfigurationBuilder {

		/**
		 * @param poolConfig must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if poolConfig is {@literal null}.
		 */
		LettucePoolingClientConfigurationBuilder poolConfig(GenericObjectPoolConfig poolConfig);

		/**
		 * Return to {@link LettuceClientConfigurationBuilder}.
		 *
		 * @return {@link LettuceClientConfigurationBuilder}.
		 */
		LettuceClientConfigurationBuilder and();

		/**
		 * Build the {@link JedisClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link JedisClientConfiguration} object.
		 */
		LettucePoolingClientConfiguration build();
	}

	/**
	 * Default {@link LettuceClientConfigurationBuilder} implementation to build an immutable
	 * {@link LettuceClientConfiguration}.
	 */
	class DefaultLettucePoolingClientConfigurationBuilder extends DefaultLettuceClientConfigurationBuilder
			implements LettucePoolingClientConfigurationBuilder {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		@Override
		public LettucePoolingClientConfigurationBuilder poolConfig(GenericObjectPoolConfig poolConfig) {

			Assert.notNull(poolConfig, "GenericObjectPoolConfig must not be null!");

			this.poolConfig = poolConfig;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#build()
		 */
		@Override
		public LettucePoolingClientConfiguration build() {
			return new DefaultLettucePoolingClientConfiguration(useSsl, verifyPeer, startTls, clientResources, clientOptions,
					timeout, shutdownTimeout, poolConfig);
		}
	}
}
