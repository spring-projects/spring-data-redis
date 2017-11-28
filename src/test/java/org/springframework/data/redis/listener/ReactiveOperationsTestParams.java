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
package org.springframework.data.redis.listener;

import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.runners.model.Statement;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.test.util.RedisClusterRule;

/**
 * Parameters for testing implementations of {@link ReactiveRedisMessageListenerContainer}
 *
 * @author Mark Paluch
 */
class ReactiveOperationsTestParams {

	public static Collection<Object[]> testParams() {

		LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder() //
				.shutdownTimeout(Duration.ZERO) //
				.clientResources(LettuceTestClientResources.getSharedClientResources()) //
				.build();

		LettucePoolingClientConfiguration poolingConfiguration = LettucePoolingClientConfiguration.builder() //
				.shutdownTimeout(Duration.ZERO) //
				.clientResources(LettuceTestClientResources.getSharedClientResources()) //
				.build();

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(standaloneConfiguration,
				clientConfiguration);
		lettuceConnectionFactory.afterPropertiesSet();

		LettuceConnectionFactory poolingConnectionFactory = new LettuceConnectionFactory(standaloneConfiguration,
				poolingConfiguration);
		poolingConnectionFactory.afterPropertiesSet();

		List<Object[]> list = Arrays.asList(new Object[][] { //
				{ lettuceConnectionFactory, "Standalone" }, //
				{ poolingConnectionFactory, "Pooled" }, //
		});

		if (clusterAvailable()) {

			RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
			clusterConfiguration.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));

			LettuceConnectionFactory lettuceClusterConnectionFactory = new LettuceConnectionFactory(clusterConfiguration,
					clientConfiguration);
			lettuceClusterConnectionFactory.afterPropertiesSet();

			list = new ArrayList<>(list);
			list.add(new Object[] { lettuceClusterConnectionFactory, "Cluster" });
		}

		return list;
	}

	private static boolean clusterAvailable() {

		try {
			new RedisClusterRule().apply(new Statement() {
				@Override
				public void evaluate() {

				}
			}, null).evaluate();
		} catch (Throwable throwable) {
			return false;
		}
		return true;
	}

}
