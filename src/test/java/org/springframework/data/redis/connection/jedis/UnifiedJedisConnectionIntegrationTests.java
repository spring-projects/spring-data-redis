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

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.UnifiedJedis;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisConnection} using {@link redis.clients.jedis.UnifiedJedis}.
 *
 * @author Tihomir Mateev
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(inheritLocations = false)
public class UnifiedJedisConnectionIntegrationTests extends JedisConnectionIntegrationTests {

	@Test
	@Override
	@Disabled("MOVE test requires the use of SELECT")
	public void testMove() {}

	@Test
	@Override
	@Disabled("SELECT not supported by UnifiedJedis")
	public void testSelect() {
		super.testSelect();
	}

	@Test
	@Override
	@Disabled("setClientName not supported by UnifiedJedis")
	void shouldSetClientName() {
		super.shouldSetClientName();
	}

	@Test
	@Override
	void testNativeConnectionIsJedis() {
		assertThat(byteConnection.getNativeConnection()).isInstanceOf(UnifiedJedis.class);
	}

}
