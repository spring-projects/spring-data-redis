/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis client configuration for jedis. This configuration provides optional configuration elements such as
 * {@link SSLSocketFactory} and {@link JedisPoolConfig} specific to jedis client features.
 * <p>
 * Providing optional elements allows a more specific configuration of the client:
 * <ul>
 * <li>Whether to use SSL</li>
 * <li>Optional {@link SSLSocketFactory}</li>
 * <li>Optional {@link SSLParameters}</li>
 * <li>Optional {@link HostnameVerifier}</li>
 * <li>Whether to use connection-pooling</li>
 * <li>Optional {@link GenericObjectPoolConfig}</li>
 * <li>Optional client name</li>
 * <li>Connect {@link Duration timeout}</li>
 * <li>Read {@link Duration timeout}</li>
 * </ul>
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see redis.clients.jedis.Jedis
 * @see org.springframework.data.redis.connection.RedisStandaloneConfiguration
 * @see org.springframework.data.redis.connection.RedisSentinelConfiguration
 * @see org.springframework.data.redis.connection.RedisClusterConfiguration
 */
public interface JedisClientConfiguration {

	/**
	 * @return {@literal true} to use SSL, {@literal false} to use unencrypted connections.
	 */
	boolean isUseSsl();

	/**
	 * @return the optional {@link SSLSocketFactory}.
	 */
	Optional<SSLSocketFactory> getSslSocketFactory();

	/**
	 * @return the optional {@link SSLParameters}.
	 */
	Optional<SSLParameters> getSslParameters();

	/**
	 * @return the optional {@link HostnameVerifier}.
	 */
	Optional<HostnameVerifier> getHostnameVerifier();

	/**
	 * @return {@literal true} to use connection-pooling.
	 */
	boolean isUsePooling();

	/**
	 * @return the optional {@link GenericObjectPoolConfig}.
	 */
	Optional<GenericObjectPoolConfig> getPoolConfig();

	/**
	 * @return the optional client name to be set with {@code CLIENT SETNAME}.
	 */
	Optional<String> getClientName();

	/**
	 * @return the connection timeout.
	 * @see java.net.Socket#connect(SocketAddress, int)
	 */
	Duration getConnectTimeout();

	/**
	 * @return the read timeout.
	 * @see java.net.Socket#setSoTimeout(int)
	 */
	Duration getReadTimeout();

	/**
	 * Creates a new {@link JedisClientConfigurationBuilder} to build {@link JedisClientConfiguration} to be used with the
	 * jedis client.
	 *
	 * @return a new {@link JedisClientConfigurationBuilder} to build {@link JedisClientConfiguration}.
	 */
	static JedisClientConfigurationBuilder builder() {
		return new DefaultJedisClientConfigurationBuilder();
	}

	/**
	 * Creates a default {@link JedisClientConfiguration}.
	 * <dl>
	 * <dt>SSL enabled</dt>
	 * <dd>no</dd>
	 * <dt>Pooling enabled</dt>
	 * <dd>no</dd>
	 * <dt>Client Name</dt>
	 * <dd>[not set]</dd>
	 * <dt>Read Timeout</dt>
	 * <dd>2000 msec</dd>
	 * <dt>Connect Timeout</dt>
	 * <dd>2000 msec</dd>
	 * </dl>
	 *
	 * @return a {@link JedisClientConfiguration} with defaults.
	 */
	static JedisClientConfiguration defaultConfiguration() {
		return builder().build();
	}

	/**
	 * Builder for {@link JedisClientConfiguration}.
	 */
	interface JedisClientConfigurationBuilder {

		/**
		 * Enable SSL connections.
		 *
		 * @return {@link JedisSslClientConfigurationBuilder}.
		 */
		JedisSslClientConfigurationBuilder useSsl();

		/**
		 * Enable connection-pooling.
		 *
		 * @return {@link JedisPoolingClientConfigurationBuilder}.
		 */
		JedisPoolingClientConfigurationBuilder usePooling();

		/**
		 * Configure a {@code clientName} to be set with {@code CLIENT SETNAME}.
		 *
		 * @param clientName must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if clientName is {@literal null}.
		 */
		JedisClientConfigurationBuilder clientName(String clientName);

		/**
		 * Configure a read timeout.
		 *
		 * @param readTimeout must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if readTimeout is {@literal null}.
		 */
		JedisClientConfigurationBuilder readTimeout(Duration readTimeout);

		/**
		 * Configure a connection timeout.
		 *
		 * @param connectTimeout must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if connectTimeout is {@literal null}.
		 */
		JedisClientConfigurationBuilder connectTimeout(Duration connectTimeout);

		/**
		 * Build the {@link JedisClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link JedisClientConfiguration} object.
		 */
		JedisClientConfiguration build();
	}

	/**
	 * Builder for Pooling-related {@link JedisClientConfiguration}.
	 */
	interface JedisPoolingClientConfigurationBuilder {

		/**
		 * @param poolConfig must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if poolConfig is {@literal null}.
		 */
		JedisPoolingClientConfigurationBuilder poolConfig(GenericObjectPoolConfig poolConfig);

		/**
		 * Return to {@link JedisClientConfigurationBuilder}.
		 *
		 * @return {@link JedisClientConfigurationBuilder}.
		 */
		JedisClientConfigurationBuilder and();

		/**
		 * Build the {@link JedisClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link JedisClientConfiguration} object.
		 */
		JedisClientConfiguration build();
	}

	/**
	 * Builder for SSL-related {@link JedisClientConfiguration}.
	 */
	interface JedisSslClientConfigurationBuilder {

		/**
		 * @param sslSocketFactory must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if sslSocketFactory is {@literal null}.
		 */
		JedisSslClientConfigurationBuilder sslSocketFactory(SSLSocketFactory sslSocketFactory);

		/**
		 * @param sslParameters must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if sslParameters is {@literal null}.
		 */
		JedisSslClientConfigurationBuilder sslParameters(SSLParameters sslParameters);

		/**
		 * @param hostnameVerifier must not be {@literal null}.
		 * @return {@literal this} builder.
		 * @throws IllegalArgumentException if hostnameVerifier is {@literal null}.
		 */
		JedisSslClientConfigurationBuilder hostnameVerifier(HostnameVerifier hostnameVerifier);

		/**
		 * Return to {@link JedisClientConfigurationBuilder}.
		 *
		 * @return {@link JedisClientConfigurationBuilder}.
		 */
		JedisClientConfigurationBuilder and();

		/**
		 * Build the {@link JedisClientConfiguration} with the configuration applied from this builder.
		 *
		 * @return a new {@link JedisClientConfiguration} object.
		 */
		JedisClientConfiguration build();
	}

	/**
	 * Default {@link JedisClientConfigurationBuilder} implementation to build an immutable
	 * {@link JedisClientConfiguration}.
	 */
	class DefaultJedisClientConfigurationBuilder implements JedisClientConfigurationBuilder,
			JedisPoolingClientConfigurationBuilder, JedisSslClientConfigurationBuilder {

		private boolean useSsl;
		private @Nullable SSLSocketFactory sslSocketFactory;
		private @Nullable SSLParameters sslParameters;
		private @Nullable HostnameVerifier hostnameVerifier;
		private boolean usePooling;
		private GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		private @Nullable String clientName;
		private Duration readTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);
		private Duration connectTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);

		private DefaultJedisClientConfigurationBuilder() {}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#useSsl()
		 */
		@Override
		public JedisSslClientConfigurationBuilder useSsl() {

			this.useSsl = true;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisSslClientConfigurationBuilder#sslSocketFactory(javax.net.ssl.SSLSocketFactory)
		 */
		@Override
		public JedisSslClientConfigurationBuilder sslSocketFactory(SSLSocketFactory sslSocketFactory) {

			Assert.notNull(sslSocketFactory, "SSLSocketFactory must not be null!");

			this.sslSocketFactory = sslSocketFactory;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisSslClientConfigurationBuilder#sslParameters(javax.net.ssl.SSLParameters)
		 */
		@Override
		public JedisSslClientConfigurationBuilder sslParameters(SSLParameters sslParameters) {

			Assert.notNull(sslParameters, "SSLParameters must not be null!");

			this.sslParameters = sslParameters;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisSslClientConfigurationBuilder#hostnameVerifier(javax.net.ssl.HostnameVerifier)
		 */
		@Override
		public JedisSslClientConfigurationBuilder hostnameVerifier(HostnameVerifier hostnameVerifier) {

			Assert.notNull(hostnameVerifier, "HostnameVerifier must not be null!");

			this.hostnameVerifier = hostnameVerifier;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#usePooling()
		 */
		@Override
		public JedisPoolingClientConfigurationBuilder usePooling() {

			this.usePooling = true;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisPoolingClientConfigurationBuilder#poolConfig(org.apache.commons.pool2.impl.GenericObjectPoolConfig)
		 */
		@Override
		public JedisPoolingClientConfigurationBuilder poolConfig(GenericObjectPoolConfig poolConfig) {

			Assert.notNull(poolConfig, "GenericObjectPoolConfig must not be null!");

			this.poolConfig = poolConfig;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisPoolingClientConfigurationBuilder#and()
		 */
		@Override
		public JedisClientConfigurationBuilder and() {
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#clientName(java.lang.String)
		 */
		@Override
		public JedisClientConfigurationBuilder clientName(String clientName) {

			Assert.hasText(clientName, "Client name must not be null or empty!");

			this.clientName = clientName;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#readTimeout(java.time.Duration)
		 */
		@Override
		public JedisClientConfigurationBuilder readTimeout(Duration readTimeout) {

			Assert.notNull(readTimeout, "Duration must not be null!");

			this.readTimeout = readTimeout;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#connectTimeout(java.time.Duration)
		 */
		@Override
		public JedisClientConfigurationBuilder connectTimeout(Duration connectTimeout) {

			Assert.notNull(connectTimeout, "Duration must not be null!");

			this.connectTimeout = connectTimeout;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder#build()
		 */
		@Override
		public JedisClientConfiguration build() {

			return new DefaultJedisClientConfiguration(useSsl, sslSocketFactory, sslParameters, hostnameVerifier, usePooling,
					poolConfig, clientName, readTimeout, connectTimeout);
		}
	}

}
