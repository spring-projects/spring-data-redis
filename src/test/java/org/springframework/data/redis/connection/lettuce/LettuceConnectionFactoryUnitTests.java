/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assert.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterConfiguration;

import com.lambdaworks.redis.cluster.RedisClusterClient;

/**
 * @author Christoph Strobl
 */
public class LettuceConnectionFactoryUnitTests {

	private static final RedisClusterConfiguration CLUSTER_CONFIG = new RedisClusterConfiguration().clusterNode(
			"127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void shouldInitClientCorrectlyWhenClusterConfigPresent() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(CLUSTER_CONFIG);
		connectionFactory.afterPropertiesSet();

		assertThat(getField(connectionFactory, "client"), instanceOf(RedisClusterClient.class));
	}
}
