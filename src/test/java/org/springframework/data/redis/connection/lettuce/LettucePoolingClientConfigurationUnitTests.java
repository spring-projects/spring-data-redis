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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;

/**
 * Unit tests for {@link LettucePoolingClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettucePoolingClientConfigurationUnitTests {

	@Test // DATAREDIS-667
	public void shouldCreateEmptyConfiguration() {

		LettucePoolingClientConfiguration configuration = LettucePoolingClientConfiguration.defaultConfiguration();

		assertThat(configuration.getPoolConfig()).isNotNull();
		assertThat(configuration.isUseSsl()).isFalse();
		assertThat(configuration.isVerifyPeer()).isTrue();
		assertThat(configuration.isStartTls()).isFalse();
		assertThat(configuration.getClientOptions()).isEmpty();
		assertThat(configuration.getClientResources()).isEmpty();
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofMillis(100));
	}

	@Test // DATAREDIS-667
	public void shouldConfigureAllProperties() {

		ClientOptions clientOptions = ClientOptions.create();
		ClientResources sharedClientResources = LettuceTestClientResources.getSharedClientResources();
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		LettucePoolingClientConfiguration configuration = LettucePoolingClientConfiguration.builder() //
				.useSsl() //
				.disablePeerVerification() //
				.startTls().and() //
				.poolConfig(poolConfig) //
				.clientOptions(clientOptions) //
				.clientResources(sharedClientResources) //
				.commandTimeout(Duration.ofMinutes(5)) //
				.shutdownTimeout(Duration.ofHours(2)) //
				.build();

		assertThat(configuration.getPoolConfig()).isEqualTo(poolConfig);
		assertThat(configuration.isUseSsl()).isTrue();
		assertThat(configuration.isVerifyPeer()).isFalse();
		assertThat(configuration.isStartTls()).isTrue();
		assertThat(configuration.getClientOptions()).contains(clientOptions);
		assertThat(configuration.getClientResources()).contains(sharedClientResources);
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofMinutes(5));
		assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofHours(2));
	}
}
