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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisJsonCommands;
import org.springframework.data.redis.connection.json.JsonPath;
import org.springframework.util.Assert;

/**
 * {@link RedisJsonCommands} implementation for Jedis Cluster.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 */
class JedisClusterJsonCommands extends JedisJsonCommands {

	private final JedisClusterConnection connection;

	JedisClusterJsonCommands(JedisClusterConnection connection) {
		super(connection);
		this.connection = connection;
	}

	@Override
	public List<byte[]> jsonMGet(JsonPath path, byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.jsonMGet(path, keys);
		}

		List<List<byte[]>> results = connection.getClusterCommandExecutor().executeMultiKeyCommand((client, key) -> {
			return super.jsonMGet(path, key);
		}, Arrays.asList(keys)).resultsAsListSortBy(keys).stream().toList();

		List<byte[]> result = new ArrayList<>();
		for (List<byte[]> list : results) {
			result.addAll(list);
		}
		return result;
	}
}
