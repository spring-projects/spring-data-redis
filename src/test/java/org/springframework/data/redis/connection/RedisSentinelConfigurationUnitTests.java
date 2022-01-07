/*
 * Copyright 2015-2022 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.jupiter.api.Test;

import org.springframework.mock.env.MockPropertySource;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link RedisSentinelConfiguration}.
 *
 * @author Christoph Strobl
 * @author Vikas Garg
 */
class RedisSentinelConfigurationUnitTests {

	private static final String HOST_AND_PORT_1 = "127.0.0.1:123";
	private static final String HOST_AND_PORT_2 = "localhost:456";
	private static final String HOST_AND_PORT_3 = "localhost:789";
	private static final String HOST_AND_NO_PORT = "localhost";

	@Test // DATAREDIS-372
	void shouldCreateRedisSentinelConfigurationCorrectlyGivenMasterAndSingleHostAndPortString() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster",
				Collections.singleton(HOST_AND_PORT_1));

		assertThat(config.getSentinels()).hasSize(1);
		assertThat(config.getSentinels()).contains(new RedisNode("127.0.0.1", 123));
	}

	@Test // DATAREDIS-372
	void shouldCreateRedisSentinelConfigurationCorrectlyGivenMasterAndMultipleHostAndPortStrings() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster",
				new HashSet<>(Arrays.asList(HOST_AND_PORT_1, HOST_AND_PORT_2, HOST_AND_PORT_3)));

		assertThat(config.getSentinels()).hasSize(3);
		assertThat(config.getSentinels()).contains(new RedisNode("127.0.0.1", 123), new RedisNode("localhost", 456),
				new RedisNode("localhost", 789));
	}

	@Test // DATAREDIS-372
	void shouldThrowExecptionOnInvalidHostAndPortString() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RedisSentinelConfiguration("mymaster", Collections.singleton(HOST_AND_NO_PORT)));
	}

	@Test // DATAREDIS-372
	void shouldThrowExceptionWhenListOfHostAndPortIsNull() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RedisSentinelConfiguration("mymaster", Collections.singleton(null)));
	}

	@Test // DATAREDIS-372
	void shouldNotFailWhenListOfHostAndPortIsEmpty() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.emptySet());

		assertThat(config.getSentinels()).isEmpty();
	}

	@Test // DATAREDIS-372
	void shouldThrowExceptionGivenNullPropertySource() {
		assertThatIllegalArgumentException().isThrownBy(() -> new RedisSentinelConfiguration(null));
	}

	@Test // DATAREDIS-372
	void shouldNotFailWhenGivenPropertySourceNotContainingRelevantProperties() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration(new MockPropertySource());

		assertThat(config.getMaster()).isNull();
		assertThat(config.getSentinels()).isEmpty();
	}

	@Test // DATAREDIS-372
	void shouldBeCreatedCorrectlyGivenValidPropertySourceWithMasterAndSingleHostPort() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);

		RedisSentinelConfiguration config = new RedisSentinelConfiguration(propertySource);

		assertThat(config.getMaster()).isNotNull();
		assertThat(config.getMaster().getName()).isEqualTo("myMaster");
		assertThat(config.getSentinels()).hasSize(1);
		assertThat(config.getSentinels()).contains(new RedisNode("127.0.0.1", 123));
	}

	@Test // DATAREDIS-372
	void shouldBeCreatedCorrectlyGivenValidPropertySourceWithMasterAndMultipleHostPort() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes",
				StringUtils.collectionToCommaDelimitedString(Arrays.asList(HOST_AND_PORT_1, HOST_AND_PORT_2, HOST_AND_PORT_3)));

		RedisSentinelConfiguration config = new RedisSentinelConfiguration(propertySource);

		assertThat(config.getSentinels()).hasSize(3);
		assertThat(config.getSentinels()).contains(new RedisNode("127.0.0.1", 123), new RedisNode("localhost", 456),
				new RedisNode("localhost", 789));
	}

	@Test // DATAREDIS-1060
	void dataNodePasswordDoesNotAffectSentinelPassword() {

		RedisPassword password = RedisPassword.of("88888888-8x8-getting-creative-now");
		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton(HOST_AND_PORT_1));
		configuration.setPassword(password);

		assertThat(configuration.getSentinelPassword()).isEqualTo(RedisPassword.none());
	}

	@Test // GH-2218
	void dataNodeUsernameDoesNotAffectSentinelUsername() {
		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton(HOST_AND_PORT_1));
		configuration.setUsername("data-admin");
		configuration.setSentinelUsername("sentinel-admin");

		assertThat(configuration.getDataNodeUsername()).isEqualTo("data-admin");
		assertThat(configuration.getSentinelUsername()).isEqualTo("sentinel-admin");
	}

	@Test // DATAREDIS-1060
	void readSentinelPasswordFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);
		propertySource.setProperty("spring.redis.sentinel.password", "computer-says-no");

		RedisSentinelConfiguration config = new RedisSentinelConfiguration(propertySource);

		assertThat(config.getSentinelPassword()).isEqualTo(RedisPassword.of("computer-says-no"));
		assertThat(config.getSentinels()).hasSize(1).contains(new RedisNode("127.0.0.1", 123));
	}

	@Test // GH-2218
	void readSentinelUsernameFromConfigProperty() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.sentinel.master", "myMaster");
		propertySource.setProperty("spring.redis.sentinel.nodes", HOST_AND_PORT_1);
		propertySource.setProperty("spring.redis.sentinel.username", "sentinel-admin");
		propertySource.setProperty("spring.redis.sentinel.password", "foo");

		RedisSentinelConfiguration config = new RedisSentinelConfiguration(propertySource);

		assertThat(config.getSentinelUsername()).isEqualTo("sentinel-admin");
		assertThat(config.getSentinelPassword()).isEqualTo(RedisPassword.of("foo"));
		assertThat(config.getSentinels()).hasSize(1).contains(new RedisNode("127.0.0.1", 123));
	}
}
