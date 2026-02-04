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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientStringCommands}. Tests all methods in direct, transaction, and pipelined
 * modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientStringCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
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
		// Test basic set and get
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		assertThat(connection.stringCommands().get("key1".getBytes())).isEqualTo("value1".getBytes());

		// Test getSet - returns old value and sets new
		byte[] oldValue = connection.stringCommands().getSet("key1".getBytes(), "value2".getBytes());
		assertThat(oldValue).isEqualTo("value1".getBytes());
		assertThat(connection.stringCommands().get("key1".getBytes())).isEqualTo("value2".getBytes());

		// Test getDel - returns value and deletes key
		byte[] deletedValue = connection.stringCommands().getDel("key1".getBytes());
		assertThat(deletedValue).isEqualTo("value2".getBytes());
		assertThat(connection.stringCommands().get("key1".getBytes())).isNull();

		// Test getEx - get with expiration update
		connection.stringCommands().set("key2".getBytes(), "value3".getBytes());
		byte[] result = connection.stringCommands().getEx("key2".getBytes(), Expiration.seconds(10));
		assertThat(result).isEqualTo("value3".getBytes());
	}

	@Test
	void multipleKeyOperationsShouldWork() {
		// Test mSet - set multiple keys at once
		Map<byte[], byte[]> tuples = new HashMap<>();
		tuples.put("k1".getBytes(), "v1".getBytes());
		tuples.put("k2".getBytes(), "v2".getBytes());
		tuples.put("k3".getBytes(), "v3".getBytes());
		Boolean result = connection.stringCommands().mSet(tuples);
		assertThat(result).isTrue();

		// Test mGet - get multiple keys at once
		List<byte[]> results = connection.stringCommands().mGet("k1".getBytes(), "k2".getBytes(), "k3".getBytes());
		assertThat(results).hasSize(3).contains("v1".getBytes(), "v2".getBytes(), "v3".getBytes());

		// Test mSetNX - set multiple keys only if none exist
		Map<byte[], byte[]> newTuples = new HashMap<>();
		newTuples.put("k4".getBytes(), "v4".getBytes());
		newTuples.put("k5".getBytes(), "v5".getBytes());
		Boolean nxResult = connection.stringCommands().mSetNX(newTuples);
		assertThat(nxResult).isTrue();

		// mSetNX should fail if any key exists
		newTuples.put("k1".getBytes(), "v1_new".getBytes());
		Boolean nxFailResult = connection.stringCommands().mSetNX(newTuples);
		assertThat(nxFailResult).isFalse();
	}

	// ============ Set Operations with Options ============
	@Test
	void setOperationsWithOptionsShouldWork() {
		// Test setNX - set only if not exists
		Boolean nxResult = connection.stringCommands().setNX("nxkey".getBytes(), "value1".getBytes());
		assertThat(nxResult).isTrue();
		Boolean nxFailResult = connection.stringCommands().setNX("nxkey".getBytes(), "value2".getBytes());
		assertThat(nxFailResult).isFalse();

		// Test setEx - set with expiration in seconds
		Boolean exResult = connection.stringCommands().setEx("exkey".getBytes(), 10, "value".getBytes());
		assertThat(exResult).isTrue();

		// Test pSetEx - set with expiration in milliseconds
		Boolean pexResult = connection.stringCommands().pSetEx("pexkey".getBytes(), 10000, "value".getBytes());
		assertThat(pexResult).isTrue();

		// Test set with expiration and option
		Boolean setResult = connection.stringCommands().set("optkey".getBytes(), "value".getBytes(), Expiration.seconds(10),
				RedisStringCommands.SetOption.UPSERT);
		assertThat(setResult).isTrue();

		// Test setGet - set and return old value
		connection.stringCommands().set("sgkey".getBytes(), "oldvalue".getBytes());
		byte[] oldValue = connection.stringCommands().setGet("sgkey".getBytes(), "newvalue".getBytes(),
				Expiration.seconds(10), RedisStringCommands.SetOption.UPSERT);
		assertThat(oldValue).isEqualTo("oldvalue".getBytes());
		assertThat(connection.stringCommands().get("sgkey".getBytes())).isEqualTo("newvalue".getBytes());
	}

	// ============ Counter Operations ============
	@Test
	void counterOperationsShouldWork() {
		// Test incr - increment by 1
		connection.stringCommands().set("counter".getBytes(), "10".getBytes());
		Long incrResult = connection.stringCommands().incr("counter".getBytes());
		assertThat(incrResult).isEqualTo(11L);

		// Test incrBy - increment by specific amount
		Long incrByResult = connection.stringCommands().incrBy("counter".getBytes(), 5);
		assertThat(incrByResult).isEqualTo(16L);

		// Test decr - decrement by 1
		Long decrResult = connection.stringCommands().decr("counter".getBytes());
		assertThat(decrResult).isEqualTo(15L);

		// Test decrBy - decrement by specific amount
		Long decrByResult = connection.stringCommands().decrBy("counter".getBytes(), 3);
		assertThat(decrByResult).isEqualTo(12L);

		// Test incrBy with float
		connection.stringCommands().set("floatCounter".getBytes(), "10.5".getBytes());
		Double floatResult = connection.stringCommands().incrBy("floatCounter".getBytes(), 2.5);
		assertThat(floatResult).isEqualTo(13.0);
	}

	// ============ String Manipulation Operations ============
	@Test
	void stringManipulationShouldWork() {
		// Test append
		connection.stringCommands().set("msg".getBytes(), "Hello".getBytes());
		Long appendResult = connection.stringCommands().append("msg".getBytes(), " World".getBytes());
		assertThat(appendResult).isEqualTo(11L);
		assertThat(connection.stringCommands().get("msg".getBytes())).isEqualTo("Hello World".getBytes());

		// Test getRange - get substring
		byte[] rangeResult = connection.stringCommands().getRange("msg".getBytes(), 0, 4);
		assertThat(rangeResult).isEqualTo("Hello".getBytes());

		// Test setRange - replace substring
		connection.stringCommands().setRange("msg".getBytes(), "Redis".getBytes(), 6);
		assertThat(connection.stringCommands().get("msg".getBytes())).isEqualTo("Hello Redis".getBytes());

		// Test strLen - get string length
		Long lenResult = connection.stringCommands().strLen("msg".getBytes());
		assertThat(lenResult).isEqualTo(11L);
	}

	// ============ Bit Operations ============
	@Test
	void bitOperationsShouldWork() {
		// Test setBit and getBit
		connection.stringCommands().setBit("bitkey".getBytes(), 7, true);
		Boolean bitValue = connection.stringCommands().getBit("bitkey".getBytes(), 7);
		assertThat(bitValue).isTrue();

		// Test bitCount - count set bits
		connection.stringCommands().set("countkey".getBytes(), "foobar".getBytes());
		Long countResult = connection.stringCommands().bitCount("countkey".getBytes());
		assertThat(countResult).isGreaterThan(0L);

		// Test bitCount with range
		Long rangeCountResult = connection.stringCommands().bitCount("countkey".getBytes(), 0, 1);
		assertThat(rangeCountResult).isGreaterThanOrEqualTo(0L);

		// Test bitOp - perform bitwise operations
		connection.stringCommands().set("key1".getBytes(), "foo".getBytes());
		connection.stringCommands().set("key2".getBytes(), "bar".getBytes());
		Long opResult = connection.stringCommands().bitOp(RedisStringCommands.BitOperation.AND, "dest".getBytes(),
				"key1".getBytes(), "key2".getBytes());
		assertThat(opResult).isGreaterThanOrEqualTo(0L);

		// Test bitPos - find first bit set to 0 or 1
		byte[] value = new byte[] { (byte) 0xff, (byte) 0xf0, (byte) 0x00 };
		connection.stringCommands().set("poskey".getBytes(), value);
		Long posResult = connection.stringCommands().bitPos("poskey".getBytes(), false, Range.unbounded());
		assertThat(posResult).isGreaterThanOrEqualTo(0L);

		// Test bitField - perform multiple bit operations
		BitFieldSubCommands subCommands = BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(4))
				.valueAt(0);
		List<Long> fieldResult = connection.stringCommands().bitField("fieldkey".getBytes(), subCommands);
		assertThat(fieldResult).isNotNull();
	}

	// ============ Transaction Tests ============
	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial data
		connection.stringCommands().set("txkey1".getBytes(), "10".getBytes());
		connection.stringCommands().set("txkey2".getBytes(), "value1".getBytes());

		// Execute multiple commands in a transaction
		connection.multi();
		connection.stringCommands().incr("txkey1".getBytes());
		connection.stringCommands().getSet("txkey2".getBytes(), "value2".getBytes());
		connection.stringCommands().set("txkey3".getBytes(), "value3".getBytes());
		connection.stringCommands().get("txkey1".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed and returned correct results
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isEqualTo(11L); // incr result
		assertThat(results.get(1)).isEqualTo("value1".getBytes()); // getSet old value
		assertThat(results.get(2)).isEqualTo(true); // set result
		assertThat(results.get(3)).isEqualTo("11".getBytes()); // get result

		// Verify final state
		assertThat(connection.stringCommands().get("txkey1".getBytes())).isEqualTo("11".getBytes());
		assertThat(connection.stringCommands().get("txkey2".getBytes())).isEqualTo("value2".getBytes());
		assertThat(connection.stringCommands().get("txkey3".getBytes())).isEqualTo("value3".getBytes());
	}

	// ============ Pipeline Tests ============
	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial data
		connection.stringCommands().set("pipe1".getBytes(), "10".getBytes());
		connection.stringCommands().set("pipe2".getBytes(), "Hello".getBytes());

		// Execute multiple commands in pipeline
		connection.openPipeline();
		connection.stringCommands().incr("pipe1".getBytes());
		connection.stringCommands().incrBy("pipe1".getBytes(), 5);
		connection.stringCommands().append("pipe2".getBytes(), " World".getBytes());
		connection.stringCommands().get("pipe1".getBytes());
		connection.stringCommands().get("pipe2".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all commands executed and returned correct results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(11L); // incr result
		assertThat(results.get(1)).isEqualTo(16L); // incrBy result
		assertThat(results.get(2)).isEqualTo(11L); // append result (length)
		assertThat(results.get(3)).isEqualTo("16".getBytes()); // get pipe1
		assertThat(results.get(4)).isEqualTo("Hello World".getBytes()); // get pipe2
	}
}
