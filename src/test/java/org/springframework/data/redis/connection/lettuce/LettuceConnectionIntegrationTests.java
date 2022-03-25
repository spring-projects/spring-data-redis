/*
 * Copyright 2011-2022 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;
import org.springframework.data.redis.test.condition.LongRunningTest;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link LettuceConnection}
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
public class LettuceConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@LongRunningTest
	void testMultiThreadsOneBlocking() throws Exception {
		Thread th = new Thread(() -> {
			DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(connectionFactory.getConnection());
			conn2.openPipeline();
			conn2.bLPop(3, "multilist");
			conn2.closePipeline();
			conn2.close();
		});
		th.start();
		Thread.sleep(1000);
		connection.set("heythere", "hi");
		th.join();
		assertThat(connection.get("heythere")).isEqualTo("hi");
	}

	@Test
	void testMultiConnectionsOneInTx() throws Exception {
		connection.set("txs1", "rightnow");
		connection.multi();
		connection.set("txs1", "delay");
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(connectionFactory.getConnection());

		// We get immediate results executing command in separate conn (not part
		// of tx)
		conn2.set("txs2", "hi");
		assertThat(conn2.get("txs2")).isEqualTo("hi");

		// Transactional value not yet set
		assertThat(conn2.get("txs1")).isEqualTo("rightnow");
		connection.exec();

		// Now it should be set
		assertThat(conn2.get("txs1")).isEqualTo("delay");
		conn2.closePipeline();
		conn2.close();
	}

	@Test
	void testCloseInTransaction() {
		connection.multi();
		connection.close();
		try {
			connection.exec();
			fail("Expected exception resuming tx");
		} catch (RedisSystemException e) {
			// expected, can't resume tx after closing conn
		}
	}

	@Test
	void testCloseBlockingOps() {
		connection.lPush("what", "baz");
		connection.bLPop(1, "what".getBytes());
		connection.close();

		// can still operate on shared conn
		connection.lPush("what", "boo");

		try {
			// can't do blocking ops after closing
			connection.bLPop(1, "what".getBytes());
			fail("Expected exception using a closed conn for dedicated ops");
		} catch (RedisSystemException e) {}
	}

	@Test
	void testCloseNonPooledConnectionNotShared() {
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory2.setShutdownTimeout(0);
		factory2.setShareNativeConnection(false);
		factory2.afterPropertiesSet();
		RedisConnection connection = factory2.getConnection();
		// Use the connection to make sure the channel is initialized, else nothing happens on close
		connection.ping();
		connection.close();
		// The dedicated connection should be closed
		try {
			connection.set("foo".getBytes(), "bar".getBytes());
			fail("Exception should be thrown trying to use a closed connection");
		} catch (RedisSystemException e) {}
		factory2.destroy();
	}

	@Test
	public void testSelect() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> super.testSelect());
	}

	@Test
	public void testMove() {
		connection.set("foo", "bar");
		actual.add(connection.move("foo", 1));
		verifyResults(Arrays.asList(new Object[] { true }));
		// Lettuce does not support select when using shared conn, use a new conn factory
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory();
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory2.setShutdownTimeout(0);
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection conn2 = new DefaultStringRedisConnection(factory2.getConnection());
		try {
			assertThat(conn2.get("foo")).isEqualTo("bar");
		} finally {
			if (conn2.exists("foo")) {
				conn2.del("foo");
			}
			conn2.close();
			factory2.destroy();
		}
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

	@Test // DATAREDIS-106
	void zRangeByScoreTest() {

		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<String> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		assertThat(zRangeByScore.iterator().next()).isEqualTo("two");
	}

	@Test // DATAREDIS-348
	@EnabledOnRedisSentinelAvailable
	void shouldReturnSentinelCommandsWhenWhenActiveSentinelFound() {

		((LettuceConnection) byteConnection).setSentinelConfiguration(
				new RedisSentinelConfiguration().master("mymaster").sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380));
		assertThat(connection.getSentinelConnection()).isNotNull();
	}
}
