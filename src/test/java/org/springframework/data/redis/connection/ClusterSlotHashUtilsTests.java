/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.junit.Assert.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.redis.test.util.RedisClusterRule;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ClusterSlotHashUtilsTests {

	public static @ClassRule RedisClusterRule requiresCluster = new RedisClusterRule();

	@Test
	public void localCalculationShoudMatchServers() throws IOException {

		JedisCluster cluster = null;
		try {
			cluster = new JedisCluster(Collections.singleton(new HostAndPort("127.0.0.1", 7379)));

			JedisPool pool = cluster.getClusterNodes().values().iterator().next();
			Jedis jedis = pool.getResource();
			for (int i = 0; i < 10000; i++) {

				String key = randomString();
				int slot = ClusterSlotHashUtil.calculateSlot(key);
				Long serverSlot = jedis.clusterKeySlot(key);

				assertEquals(
						String.format("Expected slot for key '%s' to be %s but server calculated %s.", key, slot, serverSlot),
						serverSlot.intValue(), slot);

			}
			jedis.close();
		} finally {
			if (cluster != null) {
				cluster.close();
			}
		}

	}

	@Test
	public void localCalculationShoudMatchServersForPrefixedKeys() throws IOException {

		JedisCluster cluster = null;
		try {
			cluster = new JedisCluster(Collections.singleton(new HostAndPort("127.0.0.1", 7379)));

			JedisPool pool = cluster.getClusterNodes().values().iterator().next();
			Jedis jedis = pool.getResource();
			for (int i = 0; i < 10000; i++) {

				String slotPrefix = "{" + randomString() + "}";

				String key1 = slotPrefix + "." + randomString();
				String key2 = slotPrefix + "." + randomString();

				int slot1 = ClusterSlotHashUtil.calculateSlot(key1);
				int slot2 = ClusterSlotHashUtil.calculateSlot(key2);

				assertEquals(String.format("Expected slot for prefixed keys '%s' and '%s' to be %s but was  %s.", key1, key2,
						slot1, slot2), slot1, slot2);

				Long serverSlot1 = jedis.clusterKeySlot(key1);
				Long serverSlot2 = jedis.clusterKeySlot(key2);

				assertEquals(
						String.format("Expected slot for key '%s' to be %s but server calculated %s.", key1, slot1, serverSlot1),
						serverSlot1.intValue(), slot1);
				assertEquals(
						String.format("Expected slot for key '%s' to be %s but server calculated %s.", key2, slot2, serverSlot2),
						serverSlot1.intValue(), slot1);

			}
			jedis.close();
		} finally {
			if (cluster != null) {
				cluster.close();
			}
		}
	}

	/**
	 * Generate random string using ascii chars {@code ' ' (space)} to {@code 'z'}. Explicitly skipping { and }.
	 *
	 * @return
	 */
	private String randomString() {

		int leftLimit = 32; // letter ' ' (space)
		int rightLimit = 122; // letter 'z' (tilde)
		int targetStringLength = 0;
		while (targetStringLength == 0) {
			targetStringLength = new Random().nextInt(100);
		}

		StringBuilder buffer = new StringBuilder(targetStringLength);
		for (int i = 0; i < targetStringLength; i++) {
			int randomLimitedInt = leftLimit + (int) (new Random().nextFloat() * (rightLimit - leftLimit));
			buffer.append((char) randomLimitedInt);
		}

		String result = buffer.toString();
		if (StringUtils.hasText(result)) {
			return result;
		}
		return randomString();
	}
}
