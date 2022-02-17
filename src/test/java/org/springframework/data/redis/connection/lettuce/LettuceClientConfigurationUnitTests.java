/*
 * Copyright 2017-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.test.extension.LettuceTestClientResources;

/**
 * Unit tests for {@link LettuceClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Yanming Zhou
 */
class LettuceClientConfigurationUnitTests {

	@Test // DATAREDIS-574, DATAREDIS-576, DATAREDIS-667, DATAREDIS-918
	void shouldCreateEmptyConfiguration() {

		LettuceClientConfiguration configuration = LettuceClientConfiguration.defaultConfiguration();


		assertThat(configuration.isUseSsl()).isFalse();
		assertThat(configuration.isVerifyPeer()).isTrue();
		assertThat(configuration.isStartTls()).isFalse();
		assertThat(configuration.getClientOptions()).hasValueSatisfying(actual -> {

			TimeoutOptions timeoutOptions = actual.getTimeoutOptions();
			assertThat(timeoutOptions.isTimeoutCommands()).isTrue();
		});
		assertThat(configuration.getClientResources()).isEmpty();
		assertThat(configuration.getClientName()).isEmpty();
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofSeconds(60));
		assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofMillis(100));
		assertThat(configuration.getShutdownQuietPeriod()).isEqualTo(Duration.ofMillis(100));
	}

	@Test // DATAREDIS-574, DATAREDIS-576, DATAREDIS-667
	void shouldConfigureAllProperties() {

		ClientOptions clientOptions = ClientOptions.create();
		ClientResources sharedClientResources = LettuceTestClientResources.getSharedClientResources();

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder() //
				.useSsl() //
				.disablePeerVerification() //
				.startTls().and() //
				.clientOptions(clientOptions) //
				.clientResources(sharedClientResources) //
				.clientName("foo") //
				.commandTimeout(Duration.ofMinutes(5)) //
				.shutdownTimeout(Duration.ofHours(2)) //
				.shutdownQuietPeriod(Duration.ofMinutes(5)) //
				.build();

		assertThat(configuration.isUseSsl()).isTrue();
		assertThat(configuration.isVerifyPeer()).isFalse();
		assertThat(configuration.isStartTls()).isTrue();
		assertThat(configuration.getClientOptions()).contains(clientOptions);
		assertThat(configuration.getClientResources()).contains(sharedClientResources);
		assertThat(configuration.getClientName()).contains("foo");
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofMinutes(5));
		assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofHours(2));
		assertThat(configuration.getShutdownQuietPeriod()).isEqualTo(Duration.ofMinutes(5));
	}

	@Test // DATAREDIS-881
	void shutdownQuietPeriodShouldDefaultToTimeout() {

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder()
				.shutdownTimeout(Duration.ofSeconds(42)).build();

		assertThat(configuration.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(42));
		assertThat(configuration.getShutdownQuietPeriod()).isEqualTo(Duration.ofSeconds(42));
	}

	@Test // DATAREDIS-576
	void clientConfigurationThrowsExceptionForNullClientName() {
		assertThatIllegalArgumentException().isThrownBy(() -> LettuceClientConfiguration.builder().clientName(null));
	}

	@Test // DATAREDIS-576
	void clientConfigurationThrowsExceptionForEmptyClientName() {
		assertThatIllegalArgumentException().isThrownBy(() -> LettuceClientConfiguration.builder().clientName(" "));
	}

	@Test // GH-2116
	void shouldApplySettingsFromRedisURI() {

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder() //
				.apply(RedisURI.create("rediss://foo?verifyPeer=FULL&clientName=bar&timeout=10s")).build();

		assertThat(configuration.isUseSsl()).isTrue();
		assertThat(configuration.isVerifyPeer()).isTrue();
		assertThat(configuration.isStartTls()).isFalse();
		assertThat(configuration.getClientName()).contains("bar");
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofSeconds(10));
	}

	@Test // GH-2116
	void emptySettingsRetainClientConfiguration() {

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder() //
				.clientName("hello") //
				.commandTimeout(Duration.ofMillis(1)) //
				.apply(RedisURI.create("rediss://foo")).build();

		assertThat(configuration.getClientName()).contains("hello");
		assertThat(configuration.getCommandTimeout()).isEqualTo(Duration.ofMillis(1));
	}
}
