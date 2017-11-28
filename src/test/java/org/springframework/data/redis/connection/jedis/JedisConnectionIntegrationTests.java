/*
 * Copyright 2011-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.hamcrest.core.AllOf;
import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.test.util.RedisSentinelRule;
import org.springframework.data.redis.test.util.RedisSentinelRule.SentinelsAvailable;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.data.redis.test.util.RequiresRedisSentinel;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link JedisConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author David Liu
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	public @Rule RedisSentinelRule sentinelRule = RedisSentinelRule.withDefaultConfig().dynamicModeSelection();

	@After
	public void tearDown() {
		try {
			connection.flushAll();
		} catch (Exception e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused
			// by null key/value tests
			// Attempting to flush the DB or close the connection will result in
			// error on sending QUIT to Redis
		}

		try {
			connection.close();
		} catch (Exception e) {
			// silently close connection
		}

		connection = null;
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayBytes() {
		getResults();
		byte[] sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}").getBytes();
		initConnection();
		actual.add(byteConnection.evalSha(sha1, ReturnType.MULTI, 1, "key1".getBytes(), "arg1".getBytes()));
		List<Object> results = getResults();
		List<byte[]> scriptResults = (List<byte[]>) results.get(0);
		assertEquals(Arrays.asList(new Object[] { "key1", "arg1" }),
				Arrays.asList(new Object[] { new String(scriptResults.get(0)), new String(scriptResults.get(1)) }));
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

	@Test(expected = RedisConnectionFailureException.class) // DATAREDIS-714
	public void testCreateConnectionWithDbFailure() {

		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(77);
		factory2.afterPropertiesSet();

		try {
			factory2.getConnection();
		} finally {
			factory2.destroy();
		}
	}

	@Test
	public void testClosePool() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);
		config.setMaxIdle(1);

		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();

		RedisConnection conn2 = factory2.getConnection();
		conn2.close();
		factory2.getConnection();
		factory2.destroy();
	}

	@Test
	public void testZAddSameScores() {
		Set<StringTuple> strTuples = new HashSet<>();
		strTuples.add(new DefaultStringTuple("Bob".getBytes(), "Bob", 2.0));
		strTuples.add(new DefaultStringTuple("James".getBytes(), "James", 2.0));
		Long added = connection.zAdd("myset", strTuples);
		assertEquals(2L, added.longValue());
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalArrayScriptError() {
		super.testEvalArrayScriptError();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayError() {
		super.testEvalShaArrayError();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testExecWithoutMulti() {
		super.testExecWithoutMulti();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testErrorInTx() {
		super.testErrorInTx();
	}

	/**
	 * Override pub/sub test methods to use a separate connection factory for subscribing threads, due to this issue:
	 * https://github.com/xetorthio/jedis/issues/445
	 */
	@Test
	public void testPubSubWithNamedChannels() throws Exception {

		final String expectedChannel = "channel1";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<>();

		MessageListener listener = (message, pattern) -> {
			messages.add(message);
		};

		Thread t = new Thread() {
			{
				setDaemon(true);
			}

			public void run() {

				RedisConnection con = connectionFactory.getConnection();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				con.publish(expectedChannel.getBytes(), expectedMessage.getBytes());

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				/*
				   In some clients, unsubscribe happens async of message
				receipt, so not all
				messages may be received if unsubscribing now.
				Connection.close in teardown
				will take care of unsubscribing.
				*/
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					connection.getSubscription().unsubscribe();
				}
				con.close();
			}
		};
		t.start();

		connection.subscribe(listener, expectedChannel.getBytes());

		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		assertEquals(expectedChannel, new String(message.getChannel()));
	}

	@Test
	public void testPubSubWithPatterns() throws Exception {

		final String expectedPattern = "channel*";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<>();

		final MessageListener listener = (message, pattern) -> {
			assertEquals(expectedPattern, new String(pattern));
			messages.add(message);
		};

		Thread th = new Thread() {
			{
				setDaemon(true);
			}

			public void run() {

				// open a new connection
				RedisConnection con = connectionFactory.getConnection();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				con.publish("channel1".getBytes(), expectedMessage.getBytes());
				con.publish("channel2".getBytes(), expectedMessage.getBytes());

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				con.close();
				// In some clients, unsubscribe happens async of message
				// receipt, so not all
				// messages may be received if unsubscribing now.
				// Connection.close in teardown
				// will take care of unsubscribing.
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					connection.getSubscription().pUnsubscribe(expectedPattern.getBytes());
				}
			}
		};
		th.start();

		connection.pSubscribe(listener, expectedPattern);
		// Not all providers block on subscribe (Lettuce does not), give some
		// time for messages to be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
		message = messages.poll(5, TimeUnit.SECONDS);
		assertNotNull(message);
		assertEquals(expectedMessage, new String(message.getBody()));
	}

	@Test
	public void testPoolNPE() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);

		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setUsePool(true);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();

		RedisConnection conn = factory2.getConnection();
		try {
			conn.get(null);
		} catch (Exception e) {}
		conn.close();
		// Make sure we don't end up with broken connection
		factory2.getConnection().dbSize();
		factory2.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-285
	public void testExecuteShouldConvertArrayReplyCorrectly() {
		connection.set("spring", "awesome");
		connection.set("data", "cool");
		connection.set("redis", "supercalifragilisticexpialidocious");

		assertThat(
				(Iterable<byte[]>) connection.execute("MGET", "spring".getBytes(), "data".getBytes(), "redis".getBytes()),
				AllOf.allOf(IsInstanceOf.instanceOf(List.class), IsCollectionContaining.hasItems("awesome".getBytes(),
						"cool".getBytes(), "supercalifragilisticexpialidocious".getBytes())));
	}

	@Test // DATAREDIS-286, DATAREDIS-564
	public void expireShouldSupportExiprationForValuesLargerThanInteger() {

		connection.set("expireKey", "foo");

		long seconds = ((long) Integer.MAX_VALUE) + 1;
		connection.expire("expireKey", seconds);
		long ttl = connection.ttl("expireKey");

		assertThat(ttl, is(seconds));
	}

	@Test // DATAREDIS-286
	public void pExpireShouldSupportExiprationForValuesLargerThanInteger() {

		connection.set("pexpireKey", "foo");

		long millis = ((long) Integer.MAX_VALUE) + 10;
		connection.pExpire("pexpireKey", millis);
		long ttl = connection.pTtl("pexpireKey");

		assertTrue(String.format("difference between millis=%s and ttl=%s should not be greater than 20ms but is %s",
				millis, ttl, millis - ttl), millis - ttl < 20L);
	}

	@Test // DATAREDIS-330
	@RequiresRedisSentinel(SentinelsAvailable.ONE_ACTIVE)
	public void shouldReturnSentinelCommandsWhenWhenActiveSentinelFound() {

		((JedisConnection) byteConnection).setSentinelConfiguration(
				new RedisSentinelConfiguration().master("mymaster").sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380));
		assertThat(connection.getSentinelConnection(), notNullValue());
	}

	@Test // DATAREDIS-552
	public void shouldSetClientName() {
		assertThat(connection.getClientName(), is(equalTo("jedis-client")));
	}

	@Test // DATAREDIS-106
	public void zRangeByScoreTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<String> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		assertEquals("two", zRangeByScore.iterator().next());
	}
}
