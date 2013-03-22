/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.rjc.RjcConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Base test class for AbstractConnection integration tests
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
public abstract class AbstractConnectionIntegrationTests {

	protected StringRedisConnection connection;
	protected RedisSerializer serializer = new JdkSerializationRedisSerializer();
	protected RedisSerializer stringSerializer = new StringRedisSerializer();

	private static final String listName = "test-list";
	private static final byte[] EMPTY_ARRAY = new byte[0];

	protected abstract RedisConnectionFactory getConnectionFactory();


	@Before
	public void setUp() {
		connection = new DefaultStringRedisConnection(getConnectionFactory().getConnection());
		ConnectionFactoryTracker.add(getConnectionFactory());

	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}


	@After
	public void tearDown() {
		connection.close();
		connection = null;
	}

	@Test
	public void testLPush() throws Exception {
		byte[] val = "bar".getBytes();
		Long index = connection.lPush(listName.getBytes(), val);
		if (index != null) {
			assertEquals((Long) (index + 1), connection.lPush(listName.getBytes(), val));
		}
	}

	@Test
	public void testSetAndGet() {
		String key = "foo";
		String value = "blabla";
		connection.set(key.getBytes(), value.getBytes());
		assertEquals(value, new String(connection.get(key.getBytes())));
	}

	private boolean isJredis() {
		return connection.getClass().getSimpleName().startsWith("Jredis");
	}


	@Test
	public void testByteValue() {
		String value = UUID.randomUUID().toString();
		Person person = new Person(value, value, 1, new Address(value, 2));
		String key = getClass() + ":byteValue";
		byte[] rawKey = stringSerializer.serialize(key);

		connection.set(rawKey, serializer.serialize(person));
		byte[] rawValue = connection.get(rawKey);
		assertNotNull(rawValue);
		assertEquals(person, serializer.deserialize(rawValue));
	}

	@Test
	public void testPingPong() throws Exception {
		assertEquals("PONG", connection.ping());
	}

	@Test
	public void testBitSet() throws Exception {
		String key = "bitset-test";
		connection.setBit(key, 0, false);
		connection.setBit(key, 1, true);
		assertTrue(!connection.getBit(key, 0));
		assertTrue(connection.getBit(key, 1));
	}

	@Test
	public void testInfo() throws Exception {
		Properties info = connection.info();
		assertNotNull(info);
		assertTrue("at least 5 settings should be present", info.size() >= 5);
		String version = info.getProperty("redis_version");
		assertNotNull(version);
		System.out.println(info);
	}

	@Test
	public void testNullKey() throws Exception {
		connection.decr(EMPTY_ARRAY);
		try {
			connection.decr((String) null);
		} catch (Exception ex) {
			// expected
		}
	}

	@Test
	public void testNullValue() throws Exception {
		byte[] key = UUID.randomUUID().toString().getBytes();
		connection.append(key, EMPTY_ARRAY);
		try {
			connection.append(key, null);
		} catch (DataAccessException ex) {
			// expected
		}
	}

	@Test
	public void testHashNullKey() throws Exception {
		byte[] key = UUID.randomUUID().toString().getBytes();
		connection.hExists(key, EMPTY_ARRAY);
		try {
			connection.hExists(key, null);
		} catch (DataAccessException ex) {
			// expected
		}
	}

	@Test
	public void testHashNullValue() throws Exception {
		byte[] key = UUID.randomUUID().toString().getBytes();
		byte[] field = "random".getBytes();

		connection.hSet(key, field, EMPTY_ARRAY);
		try {
			connection.hSet(key, field, null);
		} catch (DataAccessException ex) {
			// expected
		}
	}

	@Test
	public void testNullSerialization() throws Exception {
		String[] keys = new String[] { "~", "[" };
		List<String> mGet = connection.mGet(keys);
		assertEquals(2, mGet.size());
		assertNull(mGet.get(0));
		assertNull(mGet.get(1));

		StringRedisTemplate stringTemplate = new StringRedisTemplate(getConnectionFactory());
		List<String> multiGet = stringTemplate.opsForValue().multiGet(Arrays.asList(keys));
		assertEquals(2, multiGet.size());
		assertNull(multiGet.get(0));
		assertNull(multiGet.get(1));
	}

	@Test
	public void testNullCollections() throws Exception {
		connection.openPipeline();
		assertNull(connection.keys("~*"));
		assertNull(connection.hKeys("~"));
		connection.closePipeline();
	}

	@Test
	public void testPubSubWithNamedChannels() throws Exception {
		final String expectedChannel = "channel1";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<Message>();

		MessageListener listener = new MessageListener() {
			public void onMessage(Message message, byte[] pattern) {
				messages.add(message);
				System.out.println("Received message '" + new String(message.getBody()) + "'");
			}
		};

		Thread th = new Thread(new Runnable() {
			public void run() {
				// sleep 1 second to let the registration happen
				try {
					Thread.currentThread().sleep(2000);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				RedisConnection connection2 = getConnectionFactory().getConnection();
				connection2.publish(expectedChannel.getBytes(), expectedMessage.getBytes());
				connection2.close();
				// In some clients, unsubscribe happens async of message receipt, so not all
				// messages may be received if unsubscribing now. Connection.close in teardown
				// will take care of unsubscribing.
				if(!(isAsync())) {
				  connection.getSubscription().unsubscribe();
				}
			}
		});

		th.start();
		connection.subscribe(listener, expectedChannel.getBytes());
		// Not all providers block on subscribe, give some time for messages to be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		assertEquals(expectedChannel, new String(message.getChannel()));
	}

	@Test
	public void testPubSubWithPatterns() throws Exception {
		if(isRjc()) {
			// TODO Pattern matching currently broken in RJC, see DATAREDIS-120
			return;
		}
		final String expectedPattern = "channel*";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<Message>();

		final MessageListener listener = new MessageListener() {
			public void onMessage(Message message, byte[] pattern) {
				assertEquals(expectedPattern, new String(pattern));
				messages.add(message);
				System.out.println("Received message '" + new String(message.getBody()) + "'");
			}
		};

		Thread th = new Thread(new Runnable() {
			public void run() {
				// sleep 1 second to let the registration happen
				try {
					Thread.currentThread().sleep(1500);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				RedisConnection connection2 = getConnectionFactory().getConnection();
				connection2.publish("channel1".getBytes(), expectedMessage.getBytes());
				connection2.publish("channel2".getBytes(), expectedMessage.getBytes());
				connection2.close();
				// In some clients, unsubscribe happens async of message receipt, so not all
				// messages may be received if unsubscribing now. Connection.close in teardown
				// will take care of unsubscribing.
				if(!(isAsync())) {
				  connection.getSubscription().pUnsubscribe(expectedPattern.getBytes());
				}
			}
		});

		th.start();
		connection.pSubscribe(listener, expectedPattern);
		// Not all providers block on subscribe (Lettuce does not), give some time for messages to be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
	}

	//@Test
	public void testExecuteNative() throws Exception {
		//connection.execute("PiNg");
		connection.execute("ZADD", getClass() + "#testExecuteNative", "0.9090", "item");
		connection.execute("iNFo");
		connection.execute("SET ", getClass() + "testSetNative", UUID.randomUUID().toString());
	}

	@Test(expected = DataAccessException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
	}

	@Test(expected = RedisPipelineException.class)
	public void testExceptionExecuteNativeWithPipeline() throws Exception {
		connection.openPipeline();
		connection.execute("iNFo");
		connection.execute("SET ", getClass() + "testSetNative", UUID.randomUUID().toString());
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		connection.closePipeline();
	}

	@Test
	public void testExecuteNativeWithPipeline() throws Exception {
		String key1 = getClass() + "#ExecuteNativeWithPipeline#1";
		String value1 = UUID.randomUUID().toString();
		String key2 = getClass() + "#ExecuteNativeWithPipeline#2";
		String value2 = UUID.randomUUID().toString();

		connection.openPipeline();
		connection.execute("SET", key1, value1);
		connection.execute("SET", key2, value2);
		connection.execute("GET", key1);
		connection.execute("GET", key2);
		List<Object> result = connection.closePipeline();
		assertEquals(4, result.size());
		System.out.println(result.get(2));
		System.out.println(result.get(3));
		assertArrayEquals(value1.getBytes(), (byte[]) result.get(2));
		assertArrayEquals(value2.getBytes(), (byte[]) result.get(3));
	}

	@Test
	public void testHashMethod() throws Exception {
		String hash = getClass() + ":hashtest";
		String key1 = UUID.randomUUID().toString();
		String key2 = UUID.randomUUID().toString();
		connection.hSet(hash, key1, UUID.randomUUID().toString());
		connection.hSet(hash, key2, UUID.randomUUID().toString());

		Map<String, String> hashMap = connection.hGetAll(hash);
		assertTrue(hashMap.size() >= 2);
		assertTrue(hashMap.containsKey(key1));
		assertTrue(hashMap.containsKey(key2));
	}

	@Test
	public void testMultiExec() throws Exception {
		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		connection.multi();
		connection.set(key, value);
		assertNull(connection.get(key));
		List<Object> results = connection.exec();
		assertEquals(2, results.size());
		assertEquals("OK", new String((byte[])results.get(0)));
		assertEquals(new String(value), new String((byte[])results.get(1)));
	}

	@Test
	public void testMultiDiscard() throws Exception {
		connection.set("testitnow", "willdo");
		connection.multi();
		connection.set("testitnow", "notok");
		connection.discard();
		assertEquals("willdo", connection.get("testitnow"));
		// Ensure we can run a new tx after discarding previous one
		testMultiExec();
	}


	@Test
	public void testBlPopTimeout() {
		assertNull(connection.bLPop(1, "lclist"));
	}

	@Test
	public void testBlPop() {
		connection.lPush("poplist", "foo");
		connection.lPush("poplist", "bar");
		assertEquals(Arrays.asList(new String[] {"poplist", "bar"}), connection.bLPop(1, "poplist", "otherlist"));
	}

	@Test
	public void testBRPop() {
		connection.rPush("rpoplist", "bar");
		connection.rPush("rpoplist", "foo");
		assertEquals(Arrays.asList(new String[] {"rpoplist", "foo"}), connection.bRPop(1, "rpoplist"));
	}

	@Test
	public void testBRPopTimeout() {
		assertNull(connection.bRPop(1, "rclist"));
	}

	@Test
	public void testSortStore() {
		connection.del("sortlist");
		connection.rPush("sortlist", "foo");
		connection.rPush("sortlist", "bar");
		connection.rPush("sortlist", "baz");
		assertEquals(Long.valueOf(3),
				connection.sort("sortlist", new DefaultSortParameters(null, Order.ASC, true), "newlist"));
		assertEquals(Arrays.asList(new String[] {"bar", "baz", "foo"}), connection.lRange("newlist", 0, 9));
	}

	private boolean isAsync() {
		return (getConnectionFactory() instanceof LettuceConnectionFactory) ||
				(getConnectionFactory() instanceof SrpConnectionFactory);
	}

	private boolean isRjc() {
		return (getConnectionFactory() instanceof RjcConnectionFactory);
	}
}