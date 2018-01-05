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

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.JedisPoolConfig;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;

/**
 * Unit tests for {@link JedisClientConfiguration}.
 *
 * @author Mark Paluch
 */
public class JedisClientConfigurationUnitTests {

	@Test // DATAREDIS-574
	public void shouldCreateEmptyConfiguration() {

		JedisClientConfiguration configuration = JedisClientConfiguration.defaultConfiguration();

		assertThat(configuration.getClientName()).isEmpty();
		assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(configuration.getReadTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(configuration.getHostnameVerifier()).isEmpty();
		assertThat(configuration.getPoolConfig()).isPresent();
		assertThat(configuration.getSslParameters()).isEmpty();
		assertThat(configuration.getSslSocketFactory()).isEmpty();
	}

	@Test // DATAREDIS-574
	public void shouldConfigureAllProperties() throws NoSuchAlgorithmException {

		SSLParameters sslParameters = new SSLParameters();
		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();
		JedisPoolConfig poolConfig = new JedisPoolConfig();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl() //
				.hostnameVerifier(MyHostnameVerifier.INSTANCE) //
				.sslParameters(sslParameters) //
				.sslSocketFactory(socketFactory).and() //
				.clientName("my-client") //
				.connectTimeout(Duration.ofMinutes(10)) //
				.readTimeout(Duration.ofHours(5)) //
				.usePooling().poolConfig(poolConfig) //
				.build();

		assertThat(configuration.isUseSsl()).isTrue();
		assertThat(configuration.getHostnameVerifier()).contains(MyHostnameVerifier.INSTANCE);
		assertThat(configuration.getSslParameters()).contains(sslParameters);
		assertThat(configuration.getSslSocketFactory()).contains(socketFactory);

		assertThat(configuration.getClientName()).contains("my-client");
		assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofMinutes(10));
		assertThat(configuration.getReadTimeout()).isEqualTo(Duration.ofHours(5));

		assertThat(configuration.getPoolConfig()).contains(poolConfig);
	}

	enum MyHostnameVerifier implements HostnameVerifier {

		INSTANCE;

		@Override
		public boolean verify(String s, SSLSession sslSession) {
			return false;
		}
	}
}
