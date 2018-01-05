/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import org.junit.Ignore;
import reactor.test.StepVerifier;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceReactiveClusterServerCommandsTests extends LettuceReactiveClusterCommandsTestsBase {

	static final RedisClusterNode NODE1 = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
	static final RedisClusterNode NODE2 = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT);
	static final RedisClusterNode NODE3 = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT);

	@Test // DATAREDIS-659
	public void pingShouldRespondCorrectly() {
		StepVerifier.create(connection.ping(NODE1)).expectNext("PONG").verifyComplete();
	}

	@Test // DATAREDIS-659
	@Ignore("DATAREDIS-708, Causes ERR Background append only file rewriting already")
	public void bgReWriteAofShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().bgReWriteAof(NODE1)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	@Ignore("DATAREDIS-708, Causes ERR Background append only file rewriting already")
	public void bgSaveShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().bgSave(NODE1)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void lastSaveShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().lastSave(NODE1)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void saveShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().save(NODE1)).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void dbSizeShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().dbSize(NODE1)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushDbShouldRespondCorrectly() {

		StepVerifier
				.create(connection.serverCommands().flushDb() //
						.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
						.then(connection.stringCommands().set(KEY_2_BBUFFER, VALUE_2_BBUFFER))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize(NODE1)).expectNext(1L).verifyComplete();
		StepVerifier.create(connection.serverCommands().dbSize(NODE3)).expectNext(1L).verifyComplete();

		StepVerifier.create(connection.serverCommands().flushDb(NODE1)).expectNext("OK").verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize(NODE1)).expectNext(0L).verifyComplete();
		StepVerifier.create(connection.serverCommands().dbSize(NODE3)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushAllShouldRespondCorrectly() {

		StepVerifier
				.create(connection.serverCommands().flushAll() //
						.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
						.then(connection.stringCommands().set(KEY_2_BBUFFER, VALUE_2_BBUFFER))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize(NODE1)).expectNext(1L).verifyComplete();
		StepVerifier.create(connection.serverCommands().dbSize(NODE3)).expectNext(1L).verifyComplete();

		StepVerifier.create(connection.serverCommands().flushAll(NODE1)).expectNext("OK").verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize(NODE1)).expectNext(0L).verifyComplete();
		StepVerifier.create(connection.serverCommands().dbSize(NODE3)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void infoShouldRespondCorrectly() {

		StepVerifier.create(connection.serverCommands().info(NODE1)) //
				.consumeNextWith(properties -> assertThat(properties).containsKey("tcp_port")) //
				.verifyComplete();
	}

	@Test // DATAREDIS-659
	public void standaloneInfoWithSectionShouldRespondCorrectly() {

		StepVerifier.create(connection.serverCommands().info(NODE1, "server")) //
				.consumeNextWith(properties -> {
					assertThat(properties).containsKey("tcp_port").doesNotContainKey("role");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getConfigShouldRespondCorrectly() {

		StepVerifier.create(connection.serverCommands().getConfig(NODE1, "*")) //
				.consumeNextWith(properties -> {
					assertThat(properties).containsEntry("databases", "16");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-659
	public void setConfigShouldApplyConfiguration() throws InterruptedException {

		final String slowLogKey = "slowlog-max-len";

		String resetValue = connection.serverCommands().getConfig(slowLogKey).map(it -> {
			if (it.containsKey(slowLogKey)) {
				return it.get(slowLogKey);
			}
			return it.get("127.0.0.1:7379." + slowLogKey);
		}).block().toString();

		try {
			StepVerifier.create(connection.serverCommands().setConfig(slowLogKey, resetValue)) //
					.expectNext("OK") //
					.verifyComplete();

			StepVerifier.create(connection.serverCommands().setConfig(NODE1, slowLogKey, "127")) //
					.expectNext("OK") //
					.verifyComplete();

			StepVerifier.create(connection.serverCommands().getConfig(NODE1, slowLogKey)) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry(slowLogKey, "127");
					}) //
					.verifyComplete();

			StepVerifier.create(connection.serverCommands().getConfig(NODE2, slowLogKey)) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry(slowLogKey, resetValue);
					}) //
					.verifyComplete();
		} finally {
			connection.serverCommands().setConfig(slowLogKey, resetValue).block();
		}
	}

	@Test // DATAREDIS-659
	public void configResetstatShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().resetConfigStats(NODE1)).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void timeShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().time(NODE1)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getClientListShouldReportClient() {
		StepVerifier.create(connection.serverCommands().getClientList(NODE1)).expectNextCount(1).thenCancel().verify();
	}
}
