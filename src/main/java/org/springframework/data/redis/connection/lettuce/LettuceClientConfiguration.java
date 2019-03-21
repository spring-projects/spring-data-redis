/*
 * Copyright 2017 the original author or authors.
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
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

/**
 * Redis client configuration for lettuce. This configuration provides optional configuration elements such as
 * {@link ClientResources} and {@link ClientOptions} specific to Lettuce client features.
 * <p>
 * Providing optional elements allows a more specific configuration of the client:
 * <ul>
 * <li>Whether to use SSL</li>
 * <li>Whether to verify peers using SSL</li>
 * <li>Whether to use StartTLS</li>
 * <li>Optional {@link ClientResources}</li>
 * <li>Optional {@link ClientOptions}</li>
 * <li>Client {@link Duration timeout}</li>
 * <li>Shutdown {@link Duration timeout}</li>
 * </ul>
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see org.springframework.data.redis.connection.RedisStandaloneConfiguration
 * @see org.springframework.data.redis.connection.RedisSentinelConfiguration
 * @see org.springframework.data.redis.connection.RedisClusterConfiguration
 */
public interface LettuceClientConfiguration {

	/**
	 * @return {@literal true} to use SSL, {@literal false} to use unencrypted connections.
	 */
	boolean isUseSsl();

	/**
	 * @return {@literal true} to verify peers when using {@link #isUseSsl() SSL}.
	 */
	boolean isVerifyPeer();

	/**
	 * @return {@literal true} to use Start TLS ({@code true} if the first write request shouldn't be encrypted).
	 */
	boolean isStartTls();

	/**
	 * @return the optional {@link ClientResources}.
	 */
	Optional<ClientResources> getClientResources();

	/**
	 * @return the optional {@link io.lettuce.core.ClientOptions}.
	 */
	Optional<ClientOptions> getClientOptions();

	/**
	 * @return the timeout.
	 */
	Duration getCommandTimeout();

	/**
	 * @return the shutdown timeout used to close the client.
	 * @see io.lettuce.core.AbstractRedisClient#shutdown(long, long, TimeUnit)
	 */
	Duration getShutdownTimeout();

	/**
	 * Creates a new {@link LettuceClientConfigurationBuilder} to build {@link LettuceClientConfiguration} to be used with
	 * the Lettuce client.
	 *
	 * @return a new {@link LettuceClientConfigurationBuilder} to build {@link LettuceClientConfiguration}.
	 */
	static LettuceClientConfigurationBuilder builder() {
		return new DefaultLettuceClientConfigurationBuilder();
	}

	/**
	 * Creates a default {@link LettuceClientConfiguration} with:
	 * <dl>
	 * <dt>SSL</dt>
	 * <dd>no</dd>
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
	 * @return a {@link LettuceClientConfiguration} with defaults.
	 */
	static LettuceClientConfiguration defaultConfiguration() {
		return builder().build();
	}

	/**
	 * Builder for {@link LettuceClientConfiguration}.
	 */
	interface LettuceClientConfigurationBuilder {

		/**
		 * Enable SSL connections.
		 *
		 * @return {@link LettuceSslClientConfigurationBuilder}.
		 */
		LettuceSslClientConfigurationBuilder useSsl();

		/**
		 * Configure {@link ClientResources}.
		 *
		 * @param clientResources must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if clientResources is {@literal null}.
		 */
		LettuceClientConfigurationBuilder clientResources(ClientResources clientResources);

		/**
		 * Configure {@link ClientOptions}.
		 *
		 * @param clientOptions must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if clientOptions is {@literal null}.
		 */
		LettuceClientConfigurationBuilder clientOptions(ClientOptions clientOptions);

		/**
		 * Configure a command timeout.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if timeout is {@literal null}.
		 */
		LettuceClientConfigurationBuilder commandTimeout(Duration timeout);

		/**
		 * Configure a shutdown timeout.
		 *
		 * @param shutdownTimeout must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if shutdownTimeout is {@literal null}.
		 */
		LettuceClientConfigurationBuilder shutdownTimeout(Duration shutdownTimeout);

		/**
		 * Build the {@link LettuceClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link LettuceClientConfiguration} object.
		 */
		LettuceClientConfiguration build();
	}

	/**
	 * Builder for SSL-related {@link LettuceClientConfiguration}.
	 */
	interface LettuceSslClientConfigurationBuilder {

		/**
		 * Disable peer verification.
		 *
		 * @return {@literal this} builder.
		 */
		LettuceSslClientConfigurationBuilder disablePeerVerification();

		/**
		 * Enable Start TLS to send the first bytes unencrypted.
		 *
		 * @return {@literal this} builder.
		 */
		LettuceSslClientConfigurationBuilder startTls();

		/**
		 * Return to {@link LettuceClientConfigurationBuilder}.
		 *
		 * @return {@link LettuceClientConfigurationBuilder}.
		 */
		LettuceClientConfigurationBuilder and();

		/**
		 * Build the {@link LettuceClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link LettuceClientConfiguration} object.
		 */
		LettuceClientConfiguration build();
	}

	/**
	 * Default {@link LettuceClientConfigurationBuilder} implementation to build an immutable
	 * {@link LettuceClientConfiguration}.
	 */
	class DefaultLettuceClientConfigurationBuilder
			implements LettuceClientConfigurationBuilder, LettuceSslClientConfigurationBuilder {

		private boolean useSsl;
		private boolean verifyPeer = true;
		private boolean startTls;
		private ClientResources clientResources;
		private ClientOptions clientOptions;
		private Duration timeout = Duration.ofSeconds(RedisURI.DEFAULT_TIMEOUT);
		private Duration shutdownTimeout = Duration.ofMillis(100);

		private DefaultLettuceClientConfigurationBuilder() {}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#useSsl()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder useSsl() {

			this.useSsl = true;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#disablePeerVerification()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder disablePeerVerification() {

			this.verifyPeer = false;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#startTls()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder startTls() {

			this.startTls = true;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#and()
		 */
		@Override
		public LettuceClientConfigurationBuilder and() {
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#clientResources(io.lettuce.core.resource.ClientResources)
		 */
		@Override
		public LettuceClientConfigurationBuilder clientResources(ClientResources clientResources) {

			Assert.notNull(clientResources, "ClientResources must not be null!");

			this.clientResources = clientResources;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#clientOptions(io.lettuce.core.ClientOptions)
		 */
		@Override
		public LettuceClientConfigurationBuilder clientOptions(ClientOptions clientOptions) {

			Assert.notNull(clientOptions, "ClientOptions must not be null!");

			this.clientOptions = clientOptions;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#timeout(java.time.Duration)
		 */
		@Override
		public LettuceClientConfigurationBuilder commandTimeout(Duration timeout) {

			Assert.notNull(timeout, "Duration must not be null!");

			this.timeout = timeout;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#shutdownTimeout(java.time.Duration)
		 */
		@Override
		public LettuceClientConfigurationBuilder shutdownTimeout(Duration shutdownTimeout) {

			Assert.notNull(timeout, "Duration must not be null!");

			this.shutdownTimeout = shutdownTimeout;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#build()
		 */
		@Override
		public LettuceClientConfiguration build() {
			return new DefaultLettuceClientConfiguration(useSsl, verifyPeer, startTls, clientResources, clientOptions,
					timeout, shutdownTimeout);
		}
	}
}
