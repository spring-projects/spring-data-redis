/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.sync.RedisCommands;
import org.springframework.data.redis.test.util.LettuceRedisClusterClientProvider;

/**
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class LettuceReactiveCommandsTestsBase {

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";
	static final String SAME_SLOT_KEY_1 = "{key}-1";
	static final String SAME_SLOT_KEY_2 = "{key}-2";
	static final String SAME_SLOT_KEY_3 = "{key}-3";
	static final String VALUE_1 = "value-1";
	static final String VALUE_2 = "value-2";
	static final String VALUE_3 = "value-3";

	static final byte[] SAME_SLOT_KEY_1_BYTES = SAME_SLOT_KEY_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] SAME_SLOT_KEY_2_BYTES = SAME_SLOT_KEY_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] SAME_SLOT_KEY_3_BYTES = SAME_SLOT_KEY_3.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_1_BYTES = KEY_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_2_BYTES = KEY_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_3_BYTES = KEY_3.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_2_BYTES = VALUE_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_3_BYTES = VALUE_3.getBytes(Charset.forName("UTF-8"));

	static final ByteBuffer KEY_1_BBUFFER = ByteBuffer.wrap(KEY_1_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_1_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_1_BYTES);
	static final ByteBuffer VALUE_1_BBUFFER = ByteBuffer.wrap(VALUE_1_BYTES);

	static final ByteBuffer KEY_2_BBUFFER = ByteBuffer.wrap(KEY_2_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_2_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_2_BYTES);
	static final ByteBuffer VALUE_2_BBUFFER = ByteBuffer.wrap(VALUE_2_BYTES);

	static final ByteBuffer KEY_3_BBUFFER = ByteBuffer.wrap(KEY_3_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_3_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_3_BYTES);
	static final ByteBuffer VALUE_3_BBUFFER = ByteBuffer.wrap(VALUE_3_BYTES);

	@Parameterized.Parameter(value = 0) public Object clientProvider;

	LettuceReactiveRedisConnection connection;
	RedisClusterCommands<String, String> nativeCommands;

	@Parameterized.Parameters
	public static List<Object> parameters() {
		return Arrays.asList(LettuceRedisClientProvider.local(), LettuceRedisClusterClientProvider.local());
	}

	@Before
	public void setUp() {

		AbstractRedisClient abstractRedisClient = null;
		if (clientProvider instanceof LettuceRedisClientProvider) {
			abstractRedisClient = ((LettuceRedisClientProvider) clientProvider).getClient();
		} else if (clientProvider instanceof LettuceRedisClusterClientProvider) {
			abstractRedisClient = ((LettuceRedisClusterClientProvider) clientProvider).getClient();
			assumeThat(((LettuceRedisClusterClientProvider) clientProvider).test(), is(true));
		}

		if (abstractRedisClient instanceof RedisClient) {
			nativeCommands = ((RedisClient) abstractRedisClient).connect().sync();
			connection = new LettuceReactiveRedisConnection(abstractRedisClient);

		} else if (abstractRedisClient instanceof RedisClusterClient) {
			nativeCommands = ((RedisClusterClient) abstractRedisClient).connect().sync();
			connection = new LettuceReactiveRedisClusterConnection((RedisClusterClient) abstractRedisClient);
		}

	}

	@After
	public void tearDown() {

		if (nativeCommands != null) {
			flushAll();
			nativeCommands.close();
		}

		if (connection != null) {
			connection.close();
		}
	}

	private void flushAll() {
		nativeCommands.flushall();
	}

}
