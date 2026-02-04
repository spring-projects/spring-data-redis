/*
 * Copyright 2026-present the original author or authors.
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

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

import redis.clients.jedis.Connection;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for SSL/TLS configuration in {@link JedisClientConnectionFactory}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientSslConfigurationUnitTests {

	private JedisClientConnectionFactory factory;

	@AfterEach
	void tearDown() {
		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // GH-XXXX
	void shouldApplySslConfiguration() throws NoSuchAlgorithmException {

		SSLParameters sslParameters = new SSLParameters();
		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();
		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl()
				.hostnameVerifier(HttpsURLConnection.getDefaultHostnameVerifier()).sslParameters(sslParameters)
				.sslSocketFactory(socketFactory).and().clientName("my-client")
				.connectTimeout(Duration.of(10, ChronoUnit.MINUTES)).readTimeout(Duration.of(5, ChronoUnit.DAYS)).usePooling()
				.poolConfig(poolConfig).build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration(), configuration);

		assertThat(factory.getClientConfiguration()).isSameAs(configuration);
		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientName()).isEqualTo("my-client");
		assertThat(factory.getTimeout()).isEqualTo((int) Duration.of(5, ChronoUnit.DAYS).toMillis());
		assertThat(factory.getUsePool()).isTrue();
		assertThat(factory.getClientConfiguration().getPoolConfig()).hasValue(poolConfig);
	}

	@Test // GH-XXXX
	void shouldConfigureSslForStandalone() throws NoSuchAlgorithmException {

		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl().sslSocketFactory(socketFactory)
				.and().build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6380), configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getSslSocketFactory()).contains(socketFactory);
	}

	@Test // GH-XXXX
	void shouldConfigureSslWithHostnameVerification() throws NoSuchAlgorithmException {

		HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl()
				.hostnameVerifier(hostnameVerifier).and().build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6380), configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getHostnameVerifier()).contains(hostnameVerifier);
	}

	@Test // GH-XXXX
	void shouldConfigureSslWithParameters() throws NoSuchAlgorithmException {

		SSLParameters sslParameters = new SSLParameters();
		sslParameters.setProtocols(new String[] { "TLSv1.2", "TLSv1.3" });

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl().sslParameters(sslParameters)
				.and().build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6380), configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getSslParameters()).contains(sslParameters);
	}

	@Test // GH-XXXX
	void shouldConfigureSslForSentinel() throws NoSuchAlgorithmException {

		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl().sslSocketFactory(socketFactory)
				.and().build();

		RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration().master("mymaster")
				.sentinel("localhost", 26379);

		factory = new JedisClientConnectionFactory(sentinelConfig, configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getSslSocketFactory()).contains(socketFactory);
		assertThat(factory.getSentinelConfiguration()).isSameAs(sentinelConfig);
	}

	@Test // GH-XXXX
	void shouldConfigureSslForCluster() throws NoSuchAlgorithmException {

		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl().sslSocketFactory(socketFactory)
				.and().build();

		RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration().clusterNode("localhost", 7000)
				.clusterNode("localhost", 7001);

		factory = new JedisClientConnectionFactory(clusterConfig, configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getSslSocketFactory()).contains(socketFactory);
		assertThat(factory.getClusterConfiguration()).isSameAs(clusterConfig);
	}

	@Test // GH-XXXX
	void shouldConfigureAllSslOptions() throws NoSuchAlgorithmException {

		SSLParameters sslParameters = new SSLParameters();
		sslParameters.setProtocols(new String[] { "TLSv1.3" });
		sslParameters.setCipherSuites(new String[] { "TLS_AES_256_GCM_SHA384" });

		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();
		HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl().sslSocketFactory(socketFactory)
				.sslParameters(sslParameters).hostnameVerifier(hostnameVerifier).and().build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6380), configuration);

		assertThat(factory.isUseSsl()).isTrue();
		assertThat(factory.getClientConfiguration().getSslSocketFactory()).contains(socketFactory);
		assertThat(factory.getClientConfiguration().getSslParameters()).contains(sslParameters);
		assertThat(factory.getClientConfiguration().getHostnameVerifier()).contains(hostnameVerifier);
	}

	@Test // GH-XXXX
	void shouldNotUseSslByDefault() {

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6379),
				JedisClientConfiguration.defaultConfiguration());

		assertThat(factory.isUseSsl()).isFalse();
		assertThat(factory.getClientConfiguration().getSslSocketFactory()).isEmpty();
		assertThat(factory.getClientConfiguration().getSslParameters()).isEmpty();
		assertThat(factory.getClientConfiguration().getHostnameVerifier()).isEmpty();
	}

	@Test // GH-XXXX
	void shouldConfigureSslWithDeprecatedSetter() {

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().useSsl().build();

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration("localhost", 6380), clientConfig);

		assertThat(factory.isUseSsl()).isTrue();
	}
}
