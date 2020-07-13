/*
 * Copyright 2017-2020 the original author or authors.
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
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

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
		connection.ping(NODE1).as(StepVerifier::create).expectNext("PONG").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void lastSaveShouldRespondCorrectly() {
		connection.serverCommands().lastSave(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void saveShouldRespondCorrectly() {
		connection.serverCommands().save(NODE1).as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void dbSizeShouldRespondCorrectly() {
		connection.serverCommands().dbSize(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushDbShouldRespondCorrectly() {

		connection.serverCommands().flushDb() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
				.then(connection.stringCommands().set(KEY_2_BBUFFER, VALUE_2_BBUFFER)).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.serverCommands().dbSize(NODE1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		connection.serverCommands().dbSize(NODE3).as(StepVerifier::create).expectNext(1L).verifyComplete();

		connection.serverCommands().flushDb(NODE1).as(StepVerifier::create).expectNext("OK").verifyComplete();

		connection.serverCommands().dbSize(NODE1).as(StepVerifier::create).expectNext(0L).verifyComplete();
		connection.serverCommands().dbSize(NODE3).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushAllShouldRespondCorrectly() {

		connection.serverCommands().flushAll() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
				.then(connection.stringCommands().set(KEY_2_BBUFFER, VALUE_2_BBUFFER)).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.serverCommands().dbSize(NODE1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		connection.serverCommands().dbSize(NODE3).as(StepVerifier::create).expectNext(1L).verifyComplete();

		connection.serverCommands().flushAll(NODE1).as(StepVerifier::create).expectNext("OK").verifyComplete();

		connection.serverCommands().dbSize(NODE1).as(StepVerifier::create).expectNext(0L).verifyComplete();
		connection.serverCommands().dbSize(NODE3).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void infoShouldRespondCorrectly() {

		connection.serverCommands().info(NODE1).as(StepVerifier::create) //
				.consumeNextWith(properties -> assertThat(properties).containsKey("tcp_port")) //
				.verifyComplete();
	}

	@Test // DATAREDIS-659
	public void standaloneInfoWithSectionShouldRespondCorrectly() {

		connection.serverCommands().info(NODE1, "server").as(StepVerifier::create) //
				.consumeNextWith(properties -> {
					assertThat(properties).containsKey("tcp_port").doesNotContainKey("role");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getConfigShouldRespondCorrectly() {

		connection.serverCommands().getConfig(NODE1, "*").as(StepVerifier::create) //
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
			connection.serverCommands().setConfig(slowLogKey, resetValue).as(StepVerifier::create) //
					.expectNext("OK") //
					.verifyComplete();

			connection.serverCommands().setConfig(NODE1, slowLogKey, "127").as(StepVerifier::create) //
					.expectNext("OK") //
					.verifyComplete();

			connection.serverCommands().getConfig(NODE1, slowLogKey).as(StepVerifier::create) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry(slowLogKey, "127");
					}) //
					.verifyComplete();

			connection.serverCommands().getConfig(NODE2, slowLogKey).as(StepVerifier::create) //
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
		connection.serverCommands().resetConfigStats(NODE1).as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void timeShouldRespondCorrectly() {
		connection.serverCommands().time(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getClientListShouldReportClient() {
		connection.serverCommands().getClientList(NODE1).as(StepVerifier::create).expectNextCount(1).thenCancel().verify();
	}
}
