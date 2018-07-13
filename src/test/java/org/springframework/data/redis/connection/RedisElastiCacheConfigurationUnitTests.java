/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for {@link RedisStaticMasterSlaveConfiguration}.
 *
 * @author Mark Paluch
 */
public class RedisElastiCacheConfigurationUnitTests {

	@Test // DATAREDIS-762
	public void shouldCreateSingleHostConfiguration() {

		RedisStaticMasterSlaveConfiguration singleHost = new RedisStaticMasterSlaveConfiguration("localhost");

		assertThat(singleHost.getNodes()).hasSize(1);

		RedisStandaloneConfiguration node = singleHost.getNodes().get(0);

		assertThat(node.getHostName()).isEqualToIgnoringCase("localhost");
		assertThat(node.getPort()).isEqualTo(6379);
	}

	@Test // DATAREDIS-762
	public void shouldCreateMultiHostConfiguration() {

		RedisStaticMasterSlaveConfiguration multiHost = new RedisStaticMasterSlaveConfiguration("localhost");
		multiHost.node("other-host", 6479);

		assertThat(multiHost.getNodes()).hasSize(2);

		RedisStandaloneConfiguration firstNode = multiHost.getNodes().get(0);

		assertThat(firstNode.getHostName()).isEqualToIgnoringCase("localhost");
		assertThat(firstNode.getPort()).isEqualTo(6379);

		RedisStandaloneConfiguration secondNode = multiHost.getNodes().get(1);

		assertThat(secondNode.getHostName()).isEqualToIgnoringCase("other-host");
		assertThat(secondNode.getPort()).isEqualTo(6479);
	}

	@Test // DATAREDIS-762
	public void shouldApplyPasswordToNodes() {

		RedisStaticMasterSlaveConfiguration multiHost = new RedisStaticMasterSlaveConfiguration("localhost").node("other-host", 6479);

		multiHost.setPassword(RedisPassword.of("foobar"));
		multiHost.node("third", 1234);

		assertThat(multiHost.getNodes()).extracting("password").containsExactly(RedisPassword.of("foobar"),
				RedisPassword.of("foobar"), RedisPassword.of("foobar"));
	}

	@Test // DATAREDIS-762
	public void shouldApplyDatabaseToNodes() {

		RedisStaticMasterSlaveConfiguration multiHost = new RedisStaticMasterSlaveConfiguration("localhost").node("other-host", 6479);

		multiHost.setDatabase(4);
		multiHost.node("third", 1234);

		assertThat(multiHost.getNodes()).extracting("database").containsExactly(4, 4, 4);
	}
}
