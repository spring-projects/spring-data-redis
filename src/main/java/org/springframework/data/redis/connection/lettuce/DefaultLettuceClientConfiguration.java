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
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;
import java.util.Optional;

import org.springframework.lang.Nullable;

/**
 * Default implementation of {@literal LettuceClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Yanming Zhou
 * @author Zhian Chen
 * @since 2.0
 */
class DefaultLettuceClientConfiguration implements LettuceClientConfiguration {

	private final boolean useSsl;
	private final SslVerifyMode verifyMode;
	private final boolean startTls;
	private final Optional<ClientResources> clientResources;
	private final Optional<ClientOptions> clientOptions;
	private final Optional<String> clientName;
	private final Optional<ReadFrom> readFrom;
	private final Optional<RedisCredentialsProviderFactory> redisCredentialsProviderFactory;
	private final Duration timeout;
	private final Duration shutdownTimeout;
	private final Duration shutdownQuietPeriod;

	DefaultLettuceClientConfiguration(boolean useSsl, SslVerifyMode verifyMode, boolean startTls,
			@Nullable ClientResources clientResources, @Nullable ClientOptions clientOptions, @Nullable String clientName,
			@Nullable ReadFrom readFrom, @Nullable RedisCredentialsProviderFactory redisCredentialsProviderFactory,
			Duration timeout, Duration shutdownTimeout, Duration shutdownQuietPeriod) {

		this.useSsl = useSsl;
		this.verifyMode = verifyMode;
		this.startTls = startTls;
		this.clientResources = Optional.ofNullable(clientResources);
		this.clientOptions = Optional.ofNullable(clientOptions);
		this.clientName = Optional.ofNullable(clientName);
		this.readFrom = Optional.ofNullable(readFrom);
		this.redisCredentialsProviderFactory = Optional.ofNullable(redisCredentialsProviderFactory);
		this.timeout = timeout;
		this.shutdownTimeout = shutdownTimeout;
		this.shutdownQuietPeriod = shutdownQuietPeriod;
	}

	@Override
	public boolean isUseSsl() {
		return useSsl;
	}

	@Override
	public boolean isVerifyPeer() {
		return verifyMode != SslVerifyMode.NONE;
	}

	@Override
	public SslVerifyMode getVerifyMode() {
		return verifyMode;
	}

	@Override
	public boolean isStartTls() {
		return startTls;
	}

	@Override
	public Optional<ClientResources> getClientResources() {
		return clientResources;
	}

	@Override
	public Optional<ClientOptions> getClientOptions() {
		return clientOptions;
	}

	@Override
	public Optional<String> getClientName() {
		return clientName;
	}

	@Override
	public Optional<ReadFrom> getReadFrom() {
		return readFrom;
	}

	@Override
	public Optional<RedisCredentialsProviderFactory> getRedisCredentialsProviderFactory() {
		return redisCredentialsProviderFactory;
	}

	@Override
	public Duration getCommandTimeout() {
		return timeout;
	}

	@Override
	public Duration getShutdownTimeout() {
		return shutdownTimeout;
	}

	@Override
	public Duration getShutdownQuietPeriod() {
		return shutdownQuietPeriod;
	}
}
