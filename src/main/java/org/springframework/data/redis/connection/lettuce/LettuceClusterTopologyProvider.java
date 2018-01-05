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

import io.lettuce.core.cluster.RedisClusterClient;

import java.util.LinkedHashSet;

import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.util.Assert;

/**
 * Lettuce specific implementation of {@link ClusterTopologyProvider}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
class LettuceClusterTopologyProvider implements ClusterTopologyProvider {

	private final RedisClusterClient client;

	/**
	 * @param client must not be {@literal null}.
	 */
	LettuceClusterTopologyProvider(RedisClusterClient client) {

		Assert.notNull(client, "RedisClusterClient must not be null.");

		this.client = client;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ClusterTopologyProvider#getTopology()
	 */
	@Override
	public ClusterTopology getTopology() {
		return new ClusterTopology(new LinkedHashSet<>(LettuceConverters.partitionsToClusterNodes(client.getPartitions())));
	}
}
