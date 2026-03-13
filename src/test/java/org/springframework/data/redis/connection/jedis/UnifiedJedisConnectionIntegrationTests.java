/*
 * Copyright 2011-present the original author or authors.
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
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link UnifiedJedisConnection}.
 * <p>
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see UnifiedJedisConnection
 * @see JedisConnectionIntegrationTests
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class UnifiedJedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.serverCommands().flushAll();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to flush the DB or close the connection will result in error on sending QUIT to Redis
		}

		try {
			connection.close();
		} catch (Exception ignore) {}

		connection = null;
	}

	@Test
	void testConnectionIsUnifiedJedisConnection() {
		assertThat(byteConnection).isInstanceOf(UnifiedJedisConnection.class);
	}

	@Test
	void testNativeConnectionIsRedisClient() {
		assertThat(byteConnection.getNativeConnection()).isInstanceOf(redis.clients.jedis.RedisClient.class);
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

	/**
	 * SELECT is not supported with pooled connections because it contaminates the pool.
	 * When a connection in the pool has SELECT called on it, it changes the database
	 * for that specific connection. When that connection is returned to the pool, subsequent
	 * borrowers get a connection that's pointing to the wrong database.
	 */
	@Test
	@Disabled("SELECT is not supported with pooled connections")
	@Override
	public void testSelect() {}

	/**
	 * MOVE uses SELECT internally and is not supported with pooled connections.
	 */
	@Test
	@Disabled("MOVE is not supported with pooled connections")
	@Override
	public void testMove() {}

	/**
	 * setClientName is not supported with pooled connections because it contaminates the pool.
	 * Configure client name via JedisConnectionFactory.setClientName() instead.
	 */
	@Test
	@Disabled("setClientName is not supported with pooled connections - configure via JedisConnectionFactory")
	@Override
	public void clientSetNameWorksCorrectly() {}

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
     * <a href="https://github.com/xetorthio/jedis/issues/445">...</a>
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
				} catch (InterruptedException ex) {
					fail(ex.getMessage());
				}

				con.publish(expectedChannel.getBytes(), expectedMessage.getBytes());

				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					fail(ex.getMessage());
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
				} catch (InterruptedException ex) {
					fail(ex.getMessage());
				}

				con.publish("channel1".getBytes(), expectedMessage.getBytes());
				con.publish("channel2".getBytes(), expectedMessage.getBytes());

				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					fail(ex.getMessage());
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
				.describedAs("difference between millis=%s and ttl=%s should not be greater than 20ms but is %s", millis, ttl,
						millis - ttl)
				.isTrue();
	}

	@Test // DATAREDIS-552
	void shouldSetClientName() {
		assertThat(connection.getClientName()).isEqualTo("unified-jedis-client");
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

