/*
 * Copyright 2015-2025 the original author or authors.
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

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link ClusterSlotHashUtil}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
public class ClusterSlotHashUtilsTests {

	private final JedisCluster cluster;

	public ClusterSlotHashUtilsTests(JedisCluster cluster) {
		this.cluster = cluster;
	}

	@Test
	void localCalculationShouldMatchServers() {

		ConnectionPool pool = cluster.getClusterNodes().values().iterator().next();
		Jedis jedis = new Jedis(pool.getResource());

		for (int i = 0; i < 100; i++) {

			String key = randomString();
			int slot = ClusterSlotHashUtil.calculateSlot(key);
			Long serverSlot = jedis.clusterKeySlot(key);

			assertThat(slot)
					.describedAs("Expected slot for key '%s' to be %s but server calculated %s", key, slot, serverSlot)
					.isEqualTo(serverSlot.intValue());
		}

		jedis.close();
	}

	@Test
	void localCalculationShoudMatchServersForPrefixedKeys() {

		ConnectionPool pool = cluster.getClusterNodes().values().iterator().next();
		Jedis jedis = new Jedis(pool.getResource());

		for (int i = 0; i < 100; i++) {

			String slotPrefix = "{" + randomString() + "}";

			String key1 = slotPrefix + "." + randomString();
			String key2 = slotPrefix + "." + randomString();

			int slot1 = ClusterSlotHashUtil.calculateSlot(key1);
			int slot2 = ClusterSlotHashUtil.calculateSlot(key2);

			assertThat(slot2)
					.describedAs("Expected slot for prefixed keys '%s' and '%s' to be %s but was  %s.", key1, key2, slot1, slot2)
					.isEqualTo(slot1);

			Long serverSlot1 = jedis.clusterKeySlot(key1);
			Long serverSlot2 = jedis.clusterKeySlot(key2);

			assertThat(slot1)
					.describedAs("Expected slot for key '%s' to be %s but server calculated %s.", key1, slot1, serverSlot1)
					.isEqualTo(serverSlot1.intValue());

			assertThat(slot1)
					.describedAs("Expected slot for key '%s' to be %s but server calculated %s.", key2, slot2, serverSlot2)
					.isEqualTo(serverSlot1.intValue());
		}

		jedis.close();
	}

	/**
	 * Generate random {@link String} using ASCII chars {@code ' ' (space)} to {@code 'z'}. Explicitly skipping { and }.
	 *
	 * @return random {@link String} of ASCII chars.
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
