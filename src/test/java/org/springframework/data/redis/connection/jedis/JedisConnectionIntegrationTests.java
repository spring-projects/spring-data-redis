/*
 * Copyright 2011-2023 the original author or authors.
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
import static org.mockito.Mockito.*;

import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;
import org.springframework.data.redis.util.ConnectionVerifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

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
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@AfterEach
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
	public void testEvalShaArrayBytes() {
		getResults();
		byte[] sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}").getBytes();
		initConnection();
		actual.add(byteConnection.evalSha(sha1, ReturnType.MULTI, 1, "key1".getBytes(), "arg1".getBytes()));
		List<Object> results = getResults();
		List<byte[]> scriptResults = (List<byte[]>) results.get(0);
		assertThat(Arrays.asList(new Object[] { new String(scriptResults.get(0)), new String(scriptResults.get(1)) }))
				.isEqualTo(Arrays.asList(new Object[] { "key1", "arg1" }));
	}

	@Test
	void testCreateConnectionWithDb() {

		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(1);

		ConnectionVerifier.create(factory2) //
				.execute(RedisConnection::ping) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-714
	void testCreateConnectionWithDbFailure() {

		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(77);
		factory2.afterPropertiesSet();
		factory2.start();

		try {
			assertThatExceptionOfType(RedisConnectionFailureException.class).isThrownBy(factory2::getConnection);
		} finally {
			factory2.destroy();
		}
	}

	@Test
	void testClosePool() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);
		config.setMaxIdle(1);

		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();
		factory2.start();

		try {

			RedisConnection conn2 = factory2.getConnection();
			conn2.close();
			factory2.getConnection();
		} finally {
			factory2.destroy();
		}
	}

	@Test
	void testZAddSameScores() {
		Set<StringTuple> strTuples = new HashSet<>();
		strTuples.add(new DefaultStringTuple("Bob".getBytes(), "Bob", 2.0));
		strTuples.add(new DefaultStringTuple("James".getBytes(), "James", 2.0));
		Long added = connection.zAdd("myset", strTuples);
		assertThat(added.longValue()).isEqualTo(2L);
	}

	@Test
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0));
	}

	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar"));
	}

	@Test
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2"));
	}

	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1"));
	}

	@Test
	public void testRestoreBadData() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.restore("testing".getBytes(), 0, "foo".getBytes()));
	}

	@Test
	@Disabled
	public void testRestoreExistingKey() {}

	@Test
	public void testExecWithoutMulti() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> connection.exec());
	}

	@Test
	public void testErrorInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.multi();
			connection.set("foo", "bar");
			// Try to do a list op on a value
			connection.lPop("foo");
			connection.exec();
			getResults();
		});
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
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
		assertThat(new String(message.getChannel())).isEqualTo(expectedChannel);
	}

	@Test
	public void testPubSubWithPatterns() throws Exception {

		final String expectedPattern = "channel*";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<>();

		final MessageListener listener = (message, pattern) -> {
			assertThat(new String(pattern)).isEqualTo(expectedPattern);
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
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
		message = messages.poll(5, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
	}

	@Test
	void testPoolNPE() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);

		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setUsePool(true);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();
		factory2.start();

		try (RedisConnection conn = factory2.getConnection()) {
			conn.get(null);
		} catch (Exception e) {

		} finally {
			// Make sure we don't end up with broken connection
			factory2.getConnection().dbSize();
			factory2.destroy();
		}
	}

	@Test // GH-2356
	void closeWithFailureShouldReleaseConnection() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);

		JedisConnectionFactory factory = new JedisConnectionFactory(config);
		factory.setUsePool(true);
		factory.setHostName(SettingsUtils.getHost());
		factory.setPort(SettingsUtils.getPort());

		ConnectionVerifier.create(factory) //
				.execute(connection -> {
					JedisSubscription subscriptionMock = mock(JedisSubscription.class);
					doThrow(new IllegalStateException()).when(subscriptionMock).close();
					ReflectionTestUtils.setField(connection, "subscription", subscriptionMock);
				}) //
				.verifyAndRun(connectionFactory -> {
					connectionFactory.getConnection().dbSize();
					connectionFactory.destroy();
				});
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-285
	void testExecuteShouldConvertArrayReplyCorrectly() {
		connection.set("spring", "awesome");
		connection.set("data", "cool");
		connection.set("redis", "supercalifragilisticexpialidocious");

		assertThat(
				(Iterable<byte[]>) connection.execute("MGET", "spring".getBytes(), "data".getBytes(), "redis".getBytes()))
						.isInstanceOf(List.class)
						.contains("awesome".getBytes(), "cool".getBytes(), "supercalifragilisticexpialidocious".getBytes());
	}

	@Test // DATAREDIS-286, DATAREDIS-564
	void expireShouldSupportExiprationForValuesLargerThanInteger() {

		connection.set("expireKey", "foo");

		long seconds = ((long) Integer.MAX_VALUE) + 1;
		connection.expire("expireKey", seconds);
		long ttl = connection.ttl("expireKey");

		assertThat(ttl).isEqualTo(seconds);
	}

	@Test // DATAREDIS-286
	void pExpireShouldSupportExiprationForValuesLargerThanInteger() {

		connection.set("pexpireKey", "foo");

		long millis = ((long) Integer.MAX_VALUE) + 10;
		connection.pExpire("pexpireKey", millis);
		long ttl = connection.pTtl("pexpireKey");

		assertThat(millis - ttl < 20L)
				.as(String.format("difference between millis=%s and ttl=%s should not be greater than 20ms but is %s", millis,
						ttl, millis - ttl))
				.isTrue();
	}

	@Test // DATAREDIS-330
	@EnabledOnRedisSentinelAvailable
	void shouldReturnSentinelCommandsWhenWhenActiveSentinelFound() {

		((JedisConnection) byteConnection).setSentinelConfiguration(
				new RedisSentinelConfiguration().master("mymaster").sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380));
		assertThat(connection.getSentinelConnection()).isNotNull();
	}

	@Test // DATAREDIS-552
	void shouldSetClientName() {
		assertThat(connection.getClientName()).isEqualTo("jedis-client");
	}

	@Test // DATAREDIS-106
	void zRangeByScoreTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<String> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		assertThat(zRangeByScore.iterator().next()).isEqualTo("two");
	}
}
