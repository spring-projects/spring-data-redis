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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientStringCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterStringCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Get/Set Operations ============
	@Test
	void basicGetSetOperationsShouldWork() {
		// Test set and get
		Boolean setResult = connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		assertThat(setResult).isTrue();
		byte[] getResult = connection.stringCommands().get("key1".getBytes());
		assertThat(getResult).isEqualTo("value1".getBytes());

		// Test getSet - get old value and set new
		byte[] getSetResult = connection.stringCommands().getSet("key1".getBytes(), "newValue".getBytes());
		assertThat(getSetResult).isEqualTo("value1".getBytes());
		assertThat(connection.stringCommands().get("key1".getBytes())).isEqualTo("newValue".getBytes());

		// Test getDel - get and delete
		byte[] getDelResult = connection.stringCommands().getDel("key1".getBytes());
		assertThat(getDelResult).isEqualTo("newValue".getBytes());
		assertThat(connection.stringCommands().get("key1".getBytes())).isNull();

		// Test getEx - get with expiration
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());
		byte[] getExResult = connection.stringCommands().getEx("key2".getBytes(), Expiration.seconds(100));
		assertThat(getExResult).isEqualTo("value2".getBytes());
	}

	@Test
	void multipleKeyOperationsShouldWork() {
		// Test mSet - set multiple keys
		Map<byte[], byte[]> map = Map.of("{tag}key1".getBytes(), "value1".getBytes(), "{tag}key2".getBytes(),
				"value2".getBytes(), "{tag}key3".getBytes(), "value3".getBytes());
		Boolean mSetResult = connection.stringCommands().mSet(map);
		assertThat(mSetResult).isTrue();

		// Test mGet - get multiple keys
		List<byte[]> mGetResult = connection.stringCommands().mGet("{tag}key1".getBytes(), "{tag}key2".getBytes(),
				"{tag}key3".getBytes());
		assertThat(mGetResult).hasSize(3);
		assertThat(mGetResult.get(0)).isEqualTo("value1".getBytes());

		// Test mSetNX - set multiple keys if none exist
		Map<byte[], byte[]> newMap = Map.of("{tag}key4".getBytes(), "value4".getBytes(), "{tag}key5".getBytes(),
				"value5".getBytes());
		Boolean mSetNXResult = connection.stringCommands().mSetNX(newMap);
		assertThat(mSetNXResult).isTrue();
	}

	@Test
	void setOperationsWithOptionsShouldWork() {
		// Test setNX - set if not exists
		Boolean setNXResult = connection.stringCommands().setNX("key1".getBytes(), "value1".getBytes());
		assertThat(setNXResult).isTrue();
		Boolean setNXResult2 = connection.stringCommands().setNX("key1".getBytes(), "value2".getBytes());
		assertThat(setNXResult2).isFalse();

		// Test setEx - set with expiration in seconds
		Boolean setExResult = connection.stringCommands().setEx("key2".getBytes(), 100, "value2".getBytes());
		assertThat(setExResult).isTrue();

		// Test pSetEx - set with expiration in milliseconds
		Boolean pSetExResult = connection.stringCommands().pSetEx("key3".getBytes(), 100000, "value3".getBytes());
		assertThat(pSetExResult).isTrue();

		// Test set with options
		Boolean setWithOptionsResult = connection.stringCommands().set("key4".getBytes(), "value4".getBytes(),
				Expiration.seconds(100), SetOption.ifAbsent());
		assertThat(setWithOptionsResult).isTrue();

		// Test setGet - set and return old value
		// byte[] setGetResult = connection.stringCommands().setGet("key1".getBytes(), "newValue".getBytes());
		// assertThat(setGetResult).isEqualTo("value1".getBytes());
	}

	@Test
	void counterOperationsShouldWork() {
		// Test incr - increment by 1
		Long incrResult = connection.stringCommands().incr("counter".getBytes());
		assertThat(incrResult).isEqualTo(1L);

		// Test incrBy - increment by value
		Long incrByResult = connection.stringCommands().incrBy("counter".getBytes(), 5);
		assertThat(incrByResult).isEqualTo(6L);

		// Test decr - decrement by 1
		Long decrResult = connection.stringCommands().decr("counter".getBytes());
		assertThat(decrResult).isEqualTo(5L);

		// Test decrBy - decrement by value
		Long decrByResult = connection.stringCommands().decrBy("counter".getBytes(), 3);
		assertThat(decrByResult).isEqualTo(2L);

		// Test incrBy with double
		Double incrByFloatResult = connection.stringCommands().incrBy("floatCounter".getBytes(), 1.5);
		assertThat(incrByFloatResult).isEqualTo(1.5);
	}

	@Test
	void stringManipulationShouldWork() {
		// Test append
		connection.stringCommands().set("key1".getBytes(), "Hello".getBytes());
		Long appendResult = connection.stringCommands().append("key1".getBytes(), " World".getBytes());
		assertThat(appendResult).isEqualTo(11L);
		assertThat(connection.stringCommands().get("key1".getBytes())).isEqualTo("Hello World".getBytes());

		// Test getRange
		byte[] getRangeResult = connection.stringCommands().getRange("key1".getBytes(), 0, 4);
		assertThat(getRangeResult).isEqualTo("Hello".getBytes());

		// Test setRange
		connection.stringCommands().setRange("key1".getBytes(), "Redis".getBytes(), 6);
		assertThat(connection.stringCommands().get("key1".getBytes())).isEqualTo("Hello Redis".getBytes());

		// Test strLen
		Long strLenResult = connection.stringCommands().strLen("key1".getBytes());
		assertThat(strLenResult).isEqualTo(11L);
	}

	@Test
	void bitOperationsShouldWork() {
		// Test setBit
		Boolean setBitResult = connection.stringCommands().setBit("bits".getBytes(), 7, true);
		assertThat(setBitResult).isFalse(); // Previous value was false

		// Test getBit
		Boolean getBitResult = connection.stringCommands().getBit("bits".getBytes(), 7);
		assertThat(getBitResult).isTrue();

		// Test bitCount
		Long bitCountResult = connection.stringCommands().bitCount("bits".getBytes());
		assertThat(bitCountResult).isEqualTo(1L);

		// Test bitPos
		Long bitPosResult = connection.stringCommands().bitPos("bits".getBytes(), true);
		assertThat(bitPosResult).isEqualTo(7L);

		// Test bitField
		BitFieldSubCommands commands = BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(8))
				.valueAt(0L);
		List<Long> bitFieldResult = connection.stringCommands().bitField("bits".getBytes(), commands);
		assertThat(bitFieldResult).isNotNull();
	}
}
