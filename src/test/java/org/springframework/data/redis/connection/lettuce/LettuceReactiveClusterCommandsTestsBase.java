/*
 * Copyright 2016. the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assume.*;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.springframework.data.redis.test.util.LettuceRedisClusterClientProvider;

import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;

/**
 * @author Christoph Strobl
 */
public abstract class LettuceReactiveClusterCommandsTestsBase {

	public static @ClassRule LettuceRedisClusterClientProvider clientProvider = LettuceRedisClusterClientProvider.local();

	RedisClusterCommands<String, String> nativeCommands;
	LettuceReactiveRedisClusterConnection connection;

	@Before
	public void before() {
		assumeThat(clientProvider.test(), is(true));
		nativeCommands = clientProvider.getClient().connect().sync();
		connection = new LettuceReactiveRedisClusterConnection(clientProvider.getClient());
	}

	@After
	public void tearDown() {

		if (nativeCommands != null) {
			nativeCommands.flushall();

			if (nativeCommands instanceof RedisCommands) {
				((RedisCommands) nativeCommands).getStatefulConnection().close();
			}

			if (nativeCommands instanceof RedisAdvancedClusterCommands) {
				((RedisAdvancedClusterCommands) nativeCommands).getStatefulConnection().close();
			}
		}

		if (connection != null) {
			connection.close();
		}
	}

}
