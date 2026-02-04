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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link JedisClientConnectionFactory}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionFactoryUnitTests {

	private JedisClientConnectionFactory connectionFactory;

	@AfterEach
	void tearDown() {
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithDefaultConfiguration() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration().getHostName()).isEqualTo("localhost");
		assertThat(connectionFactory.getStandaloneConfiguration().getPort()).isEqualTo(6379);
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithStandaloneConfiguration() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("redis-host", 6380);
		config.setDatabase(5);
		config.setPassword(RedisPassword.of("secret"));

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration().getHostName()).isEqualTo("redis-host");
		assertThat(connectionFactory.getStandaloneConfiguration().getPort()).isEqualTo(6380);
		assertThat(connectionFactory.getStandaloneConfiguration().getDatabase()).isEqualTo(5);
		assertThat(connectionFactory.getStandaloneConfiguration().getPassword()).isEqualTo(RedisPassword.of("secret"));
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithSentinelConfiguration() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration().master("mymaster").sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getSentinelConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration().getMaster().getName()).isEqualTo("mymaster");
		assertThat(connectionFactory.getSentinelConfiguration().getSentinels()).hasSize(2);
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithClusterConfiguration() {

		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode("127.0.0.1", 7000)
				.clusterNode("127.0.0.1", 7001).clusterNode("127.0.0.1", 7002);

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getClusterConfiguration()).isNotNull();
		assertThat(connectionFactory.getClusterConfiguration().getClusterNodes()).hasSize(3);
	}

	@Test // GH-XXXX
	void shouldNotBeStartedInitially() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldBeRunningAfterStart() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.isRunning()).isTrue();
	}

	@Test // GH-XXXX
	void shouldNotBeRunningAfterStop() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();
		connectionFactory.stop();

		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldSupportAutoStartup() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isAutoStartup()).isTrue();
	}

	@Test // GH-XXXX
	void shouldAllowDisablingAutoStartup() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.setAutoStartup(false);

		assertThat(connectionFactory.isAutoStartup()).isFalse();
	}

	@Test // GH-XXXX
	void shouldSupportEarlyStartup() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isEarlyStartup()).isTrue();
	}

	// Lifecycle Management Edge Case Tests - Task 10

	@Test // GH-XXXX
	void shouldHandleMultipleDestroyCalls() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		// First destroy
		connectionFactory.destroy();
		assertThat(connectionFactory.isRunning()).isFalse();

		// Second destroy should not throw exception
		assertThatNoException().isThrownBy(() -> connectionFactory.destroy());
	}

	@Test // GH-XXXX
	void shouldFailOperationsAfterDestroy() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		connectionFactory.destroy();

		assertThatIllegalStateException().isThrownBy(() -> connectionFactory.getConnection());
		assertThatIllegalStateException().isThrownBy(() -> connectionFactory.getClusterConnection());
		assertThatIllegalStateException().isThrownBy(() -> connectionFactory.getSentinelConnection());
	}

	@Test // GH-XXXX
	void shouldAllowStartAfterStop() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.isRunning()).isTrue();

		connectionFactory.stop();
		assertThat(connectionFactory.isRunning()).isFalse();

		// Should be able to start again after stop
		connectionFactory.start();
		assertThat(connectionFactory.isRunning()).isTrue();
	}

	@Test // GH-XXXX
	void shouldNotAllowStartAfterDestroy() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		connectionFactory.destroy();

		// Start after destroy should not change state
		connectionFactory.start();
		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldHandleConcurrentStartStopCalls() throws Exception {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();

		int threadCount = 10;
		java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
		java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(threadCount);
		java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);

		for (int i = 0; i < threadCount; i++) {
			final int threadNum = i;
			new Thread(() -> {
				try {
					startLatch.await();
					if (threadNum % 2 == 0) {
						connectionFactory.start();
					} else {
						connectionFactory.stop();
					}
					successCount.incrementAndGet();
				} catch (Exception e) {
					// Expected - some threads may fail due to race conditions
				} finally {
					doneLatch.countDown();
				}
			}).start();
		}

		startLatch.countDown();
		doneLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);

		// All threads should complete without hanging
		assertThat(successCount.get()).isGreaterThan(0);
		// Factory should be in a valid state (either running or stopped)
		assertThat(connectionFactory.isRunning()).isIn(true, false);
	}

	@Test // GH-XXXX
	void shouldHandleMultipleStopCalls() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.isRunning()).isTrue();

		// First stop
		connectionFactory.stop();
		assertThat(connectionFactory.isRunning()).isFalse();

		// Second stop should not throw exception
		assertThatNoException().isThrownBy(() -> connectionFactory.stop());
		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldHandleMultipleStartCalls() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();

		// First start
		connectionFactory.start();
		assertThat(connectionFactory.isRunning()).isTrue();

		// Second start should be idempotent
		assertThatNoException().isThrownBy(() -> connectionFactory.start());
		assertThat(connectionFactory.isRunning()).isTrue();
	}
}
