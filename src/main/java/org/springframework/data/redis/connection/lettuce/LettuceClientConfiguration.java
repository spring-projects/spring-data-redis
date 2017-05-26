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
 * @since 2.0
 * @see org.springframework.data.redis.connection.RedisStandaloneConfiguration
 * @see org.springframework.data.redis.connection.RedisSentinelConfiguration
 * @see org.springframework.data.redis.connection.RedisClusterConfiguration
 */
public interface LettuceClientConfiguration {

	/**
	 * @return {@literal true} to use SSL, {@literal false} to use unencrypted connections.
	 */
	boolean useSsl();

	/**
	 * @return {@literal true} to verify peers when using {@link #useSsl() SSL}.
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
	Duration getTimeout();

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
	 * Creates an empty {@link LettuceClientConfiguration}.
	 *
	 * @return an empty {@link LettuceClientConfiguration}.
	 */
	static LettuceClientConfiguration create() {
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
		 * Use plaintext connections instead of SSL.
		 *
		 * @return {@link LettuceClientConfigurationBuilder}.
		 */
		LettuceClientConfigurationBuilder usePlaintext();

		/**
		 * Configure {@link ClientResources}.
		 *
		 * @param clientResources must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		LettuceClientConfigurationBuilder clientResources(ClientResources clientResources);

		/**
		 * Configure {@link ClientOptions}.
		 *
		 * @param clientOptions must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @see ClientOptions
		 * @see io.lettuce.core.cluster.ClusterClientOptions
		 */
		LettuceClientConfigurationBuilder clientOptions(ClientOptions clientOptions);

		/**
		 * Configure a timeout.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return {@literal this} builder.
		 */
		LettuceClientConfigurationBuilder timeout(Duration timeout);

		/**
		 * Configure a shutdown timeout.
		 *
		 * @param shutdownTimeout must not be {@literal null}.
		 * @return {@literal this} builder.
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
		 * Enable peer verification.
		 *
		 * @return {@literal this} builder.
		 */
		LettuceSslClientConfigurationBuilder verifyPeer();

		/**
		 * Enable/disable peer verification.
		 *
		 * @param verifyPeer {@literal true} to enable peer verification, {@literal false} to skip peer verification.
		 * @return {@literal this} builder.
		 */
		LettuceSslClientConfigurationBuilder verifyPeer(boolean verifyPeer);

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
		private Duration shutdownTimeout = Duration.ofSeconds(2);

		private DefaultLettuceClientConfigurationBuilder() {}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#useSsl()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder useSsl() {

			this.useSsl = true;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#usePlaintext()
		 */
		@Override
		public LettuceClientConfigurationBuilder usePlaintext() {

			this.useSsl = false;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#verifyPeer()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder verifyPeer() {
			return verifyPeer(true);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#verifyPeer(boolean)
		 */
		@Override
		public LettuceSslClientConfigurationBuilder verifyPeer(boolean verifyPeer) {

			this.verifyPeer = verifyPeer;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#startTls()
		 */
		@Override
		public LettuceSslClientConfigurationBuilder startTls() {

			this.startTls = true;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceSslClientConfigurationBuilder#and()
		 */
		@Override
		public LettuceClientConfigurationBuilder and() {
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#clientResources(io.lettuce.core.resource.ClientResources)
		 */
		@Override
		public LettuceClientConfigurationBuilder clientResources(ClientResources clientResources) {

			Assert.notNull(clientResources, "ClientResources must not be null!");

			this.clientResources = clientResources;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#clientOptions(io.lettuce.core.ClientOptions)
		 */
		@Override
		public LettuceClientConfigurationBuilder clientOptions(ClientOptions clientOptions) {

			Assert.notNull(clientOptions, "ClientOptions must not be null!");

			this.clientOptions = clientOptions;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#timeout(java.time.Duration)
		 */
		@Override
		public LettuceClientConfigurationBuilder timeout(Duration timeout) {

			Assert.notNull(timeout, "Duration must not be null!");

			this.timeout = timeout;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#shutdownTimeout(java.time.Duration)
		 */
		@Override
		public LettuceClientConfigurationBuilder shutdownTimeout(Duration shutdownTimeout) {

			Assert.notNull(timeout, "Duration must not be null!");

			this.shutdownTimeout = shutdownTimeout;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder#build()
		 */
		@Override
		public LettuceClientConfiguration build() {
			return new DefaultLettuceClientConfiguration(useSsl, verifyPeer, startTls, clientResources, clientOptions,
					timeout, shutdownTimeout);
		}
	}
}
