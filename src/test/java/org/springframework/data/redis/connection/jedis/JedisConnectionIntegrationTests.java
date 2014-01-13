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

package org.springframework.data.redis.connection.jedis;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test of {@link JedisConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@After
	public void tearDown() {
		try {
			connection.flushDb();
			connection.close();
		} catch (Exception e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused
			// by null key/value tests
			// Attempting to flush the DB or close the connection will result in
			// error on sending QUIT to Redis
		}
		connection = null;
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpire() {
		super.testPExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireKeyNotExists() {
		super.testPExpireKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAt() {
		super.testPExpireAt();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAtKeyNotExists() {
		super.testPExpireAtKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtl() {
		super.testPTtl();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtlNoExpire() {
		super.testPTtlNoExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpAndRestore() {
		super.testDumpAndRestore();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpNonExistentKey() {
		super.testDumpNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreTtl() {
		super.testRestoreTtl();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCount() {
		super.testBitCount();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountInterval() {
		super.testBitCountInterval();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountNonExistentKey() {
		super.testBitCountNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpAnd() {
		super.testBitOpAnd();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpOr() {
		super.testBitOpOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpXOr() {
		super.testBitOpXOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNot() {
		super.testBitOpNot();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNotMultipleSources() {
		super.testBitOpNotMultipleSources();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testHIncrByDouble() {
		super.testHIncrByDouble();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testIncrByDouble() {
		super.testIncrByDouble();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCount() {
		super.testSRandMemberCount();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountKeyNotExists() {
		super.testSRandMemberCountKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountNegative() {
		super.testSRandMemberCountNegative();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testInfoBySection() throws Exception {
		super.testInfoBySection();
	}

	@Test
	public void testCreateConnectionWithDb() {
		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		// No way to really verify we are in the selected DB
		factory2.getConnection().ping();
		factory2.destroy();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testCreateConnectionWithDbFailure() {
		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(77);
		factory2.afterPropertiesSet();
		factory2.getConnection();
		factory2.destroy();
	}

	@Test
	public void testClosePool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(1);
		config.setMaxWait(1l);
		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();
		RedisConnection conn2 = factory2.getConnection();
		conn2.close();
		factory2.getConnection();
		factory2.destroy();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testZAddSameScores() {
		Set<StringTuple> strTuples = new HashSet<StringTuple>();
		strTuples.add(new DefaultStringTuple("Bob".getBytes(), "Bob", 2.0));
		strTuples.add(new DefaultStringTuple("James".getBytes(), "James", 2.0));
		connection.zAdd("myset", strTuples);
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalArrayScriptError() {
		super.testEvalArrayScriptError();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaArrayError() {
		super.testEvalShaArrayError();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testExecWithoutMulti() {
		super.testExecWithoutMulti();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testErrorInTx() {
		super.testErrorInTx();
	}

	// Override pub/sub test methods to use a separate connection factory for
	// subscribing threads, due to this issue: https://github.com/xetorthio/jedis/issues/445
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

		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.setUsePool(false);
		factory2.afterPropertiesSet();
		final StringRedisConnection nonPooledConn = new DefaultStringRedisConnection(factory2.getConnection());
		Thread th = new Thread(new Runnable() {
			public void run() {
				// sleep 1/2 second to let the registration happen
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				RedisConnection connection2 = connectionFactory.getConnection();
				connection2.publish(expectedChannel.getBytes(), expectedMessage.getBytes());
				connection2.close();
				// In some clients, unsubscribe happens async of message
				// receipt, so not all
				// messages may be received if unsubscribing now.
				// Connection.close in teardown
				// will take care of unsubscribing.
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					nonPooledConn.getSubscription().unsubscribe();
				}
			}
		});
		th.start();
		nonPooledConn.subscribe(listener, expectedChannel.getBytes());
		// Not all providers block on subscribe, give some time for messages to
		// be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		assertEquals(expectedChannel, new String(message.getChannel()));
        nonPooledConn.close();
        factory2.destroy();
	}

	@Test
	public void testPubSubWithPatterns() throws Exception {
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

		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.setUsePool(false);
		factory2.afterPropertiesSet();
		final StringRedisConnection nonPooledConn = new DefaultStringRedisConnection(factory2.getConnection());

		Thread th = new Thread(new Runnable() {
			public void run() {
				// sleep 1/2 second to let the registration happen
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				RedisConnection connection2 = connectionFactory.getConnection();
				connection2.publish("channel1".getBytes(), expectedMessage.getBytes());
				connection2.publish("channel2".getBytes(), expectedMessage.getBytes());
				connection2.close();
				// In some clients, unsubscribe happens async of message
				// receipt, so not all
				// messages may be received if unsubscribing now.
				// Connection.close in teardown
				// will take care of unsubscribing.
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					nonPooledConn.getSubscription().pUnsubscribe(expectedPattern.getBytes());
				}
			}
		});

		th.start();
		nonPooledConn.pSubscribe(listener, expectedPattern);
		// Not all providers block on subscribe (Lettuce does not), give some
		// time for messages to be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
        nonPooledConn.close();
        factory2.destroy();
	}

	@Test
	public void testPoolNPE() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(1);
		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setUsePool(true);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();
		RedisConnection conn = factory2.getConnection();
		try {
			conn.get(null);
		}catch(Exception e) {
		}
		conn.close();
		// Make sure we don't end up with broken connection
		factory2.getConnection().dbSize();
        factory2.destroy();
	}
}
