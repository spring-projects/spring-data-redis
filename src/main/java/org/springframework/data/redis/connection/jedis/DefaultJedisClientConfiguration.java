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

import java.time.Duration;
import java.util.Optional;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@literal JedisClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class DefaultJedisClientConfiguration implements JedisClientConfiguration {

	private final boolean useSsl;
	private final Optional<SSLSocketFactory> sslSocketFactory;
	private final Optional<SSLParameters> sslParameters;
	private final Optional<HostnameVerifier> hostnameVerifier;
	private final boolean usePooling;
	private final Optional<GenericObjectPoolConfig> poolConfig;
	private final Optional<String> clientName;
	private final Duration readTimeout;
	private final Duration connectTimeout;

	DefaultJedisClientConfiguration(boolean useSsl, @Nullable SSLSocketFactory sslSocketFactory,
			@Nullable SSLParameters sslParameters, @Nullable HostnameVerifier hostnameVerifier, boolean usePooling,
			@Nullable GenericObjectPoolConfig poolConfig, @Nullable String clientName, Duration readTimeout,
			Duration connectTimeout) {

		this.useSsl = useSsl;
		this.sslSocketFactory = Optional.ofNullable(sslSocketFactory);
		this.sslParameters = Optional.ofNullable(sslParameters);
		this.hostnameVerifier = Optional.ofNullable(hostnameVerifier);
		this.usePooling = usePooling;
		this.poolConfig = Optional.ofNullable(poolConfig);
		this.clientName = Optional.ofNullable(clientName);
		this.readTimeout = readTimeout;
		this.connectTimeout = connectTimeout;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#useSsl()
	 */
	@Override
	public boolean isUseSsl() {
		return useSsl;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getSslSocketFactory()
	 */
	@Override
	public Optional<SSLSocketFactory> getSslSocketFactory() {
		return sslSocketFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getSslParameters()
	 */
	@Override
	public Optional<SSLParameters> getSslParameters() {
		return sslParameters;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getHostnameVerifier()
	 */
	@Override
	public Optional<HostnameVerifier> getHostnameVerifier() {
		return hostnameVerifier;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#usePooling()
	 */
	@Override
	public boolean isUsePooling() {
		return usePooling;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getPoolConfig()
	 */
	@Override
	public Optional<GenericObjectPoolConfig> getPoolConfig() {
		return poolConfig;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getClientName()
	 */
	@Override
	public Optional<String> getClientName() {
		return clientName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getReadTimeout()
	 */
	@Override
	public Duration getReadTimeout() {
		return readTimeout;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.jedis.JedisClientConfiguration#getConnectTimeout()
	 */
	@Override
	public Duration getConnectTimeout() {
		return connectTimeout;
	}
}
