/*
 * Copyright 2017-2024 the original author or authors.
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

	private final Optional<JedisClientConfigBuilderCustomizer> customizer;
	private final boolean useSsl;
	private final Optional<SSLSocketFactory> sslSocketFactory;
	private final Optional<SSLParameters> sslParameters;
	private final Optional<HostnameVerifier> hostnameVerifier;
	private final boolean usePooling;
	private final Optional<GenericObjectPoolConfig> poolConfig;
	private final Optional<String> clientName;
	private final Duration readTimeout;
	private final Duration connectTimeout;

	DefaultJedisClientConfiguration(@Nullable JedisClientConfigBuilderCustomizer customizer, boolean useSsl,
			@Nullable SSLSocketFactory sslSocketFactory,
			@Nullable SSLParameters sslParameters, @Nullable HostnameVerifier hostnameVerifier, boolean usePooling,
			@Nullable GenericObjectPoolConfig poolConfig, @Nullable String clientName, Duration readTimeout,
			Duration connectTimeout) {

		this.customizer = Optional.ofNullable(customizer);
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

	@Override
	public Optional<JedisClientConfigBuilderCustomizer> getCustomizer() {
		return customizer;
	}

	@Override
	public boolean isUseSsl() {
		return useSsl;
	}

	@Override
	public Optional<SSLSocketFactory> getSslSocketFactory() {
		return sslSocketFactory;
	}

	@Override
	public Optional<SSLParameters> getSslParameters() {
		return sslParameters;
	}

	@Override
	public Optional<HostnameVerifier> getHostnameVerifier() {
		return hostnameVerifier;
	}

	@Override
	public boolean isUsePooling() {
		return usePooling;
	}

	@Override
	public Optional<GenericObjectPoolConfig> getPoolConfig() {
		return poolConfig;
	}

	@Override
	public Optional<String> getClientName() {
		return clientName;
	}

	@Override
	public Duration getReadTimeout() {
		return readTimeout;
	}

	@Override
	public Duration getConnectTimeout() {
		return connectTimeout;
	}
}
