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
import static org.junit.Assume.*;

import org.junit.Ignore;
import reactor.test.StepVerifier;

import org.junit.Test;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceReactiveServerCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-659
	public void pingShouldRespondCorrectly() {
		StepVerifier.create(connection.ping()).expectNext("PONG").verifyComplete();
	}

	@Test // DATAREDIS-659
	@Ignore("DATAREDIS-708, Causes ERR Background append only file rewriting already")
	public void bgReWriteAofShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().bgReWriteAof()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-667
	@Ignore("DATAREDIS-708, Causes ERR Background append only file rewriting already")
	public void bgSaveShouldRespondCorrectly() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		StepVerifier.create(connection.serverCommands().bgSave()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void lastSaveShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().lastSave()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-667
	public void saveShouldRespondCorrectly() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		StepVerifier.create(connection.serverCommands().save()).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void dbSizeShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().dbSize()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushDbShouldRespondCorrectly() {

		StepVerifier.create(connection.serverCommands().flushDb() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize()).expectNext(1L).verifyComplete();

		StepVerifier.create(connection.serverCommands().flushDb()).expectNext("OK").verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize()).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushAllShouldRespondCorrectly() {

		StepVerifier.create(connection.serverCommands().flushAll() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize()).expectNext(1L).verifyComplete();

		StepVerifier.create(connection.serverCommands().flushAll()).expectNext("OK").verifyComplete();

		StepVerifier.create(connection.serverCommands().dbSize()).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void infoShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			StepVerifier.create(connection.serverCommands().info()) //
					.consumeNextWith(properties -> {

						assertThat(properties) //
								.containsKey("127.0.0.1:7379.tcp_port") //
								.containsKey("127.0.0.1:7380.tcp_port");
					}) //
					.verifyComplete();
		} else {

			StepVerifier.create(connection.serverCommands().info()) //
					.consumeNextWith(properties -> assertThat(properties).containsKey("tcp_port")) //
					.verifyComplete();
		}
	}

	@Test // DATAREDIS-659
	public void standaloneInfoWithSectionShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			StepVerifier.create(connection.serverCommands().info("server")) //
					.consumeNextWith(properties -> {
						assertThat(properties).isNotEmpty() //
								.containsKey("127.0.0.1:7379.tcp_port") //
								.doesNotContainKey("127.0.0.1:7379.role");
					}) //
					.verifyComplete();
		} else {

			StepVerifier.create(connection.serverCommands().info("server")) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsKey("tcp_port").doesNotContainKey("role");
					}) //
					.verifyComplete();
		}
	}

	@Test // DATAREDIS-659
	public void getConfigShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			StepVerifier.create(connection.serverCommands().getConfig("*")) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry("127.0.0.1:7379.databases", "16");

					}) //
					.verifyComplete();
		} else {

			StepVerifier.create(connection.serverCommands().getConfig("*")) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry("databases", "16");
					}) //
					.verifyComplete();
		}
	}

	@Test // DATAREDIS-659
	public void setConfigShouldApplyConfiguration() {

		final String slowLogKey = "slowlog-max-len";

		String resetValue = connection.serverCommands().getConfig(slowLogKey).map(it -> {
			if (it.containsKey(slowLogKey)) {
				return it.get(slowLogKey);
			}
			return it.get("127.0.0.1:7379." + slowLogKey);
		}).block().toString();

		try {
			StepVerifier.create(connection.serverCommands().setConfig(slowLogKey, "127")) //
					.expectNext("OK") //
					.verifyComplete();

			if (connection instanceof LettuceReactiveRedisClusterConnection) {
				StepVerifier.create(connection.serverCommands().getConfig(slowLogKey)) //
						.consumeNextWith(properties -> {
							assertThat(properties).containsEntry("127.0.0.1:7379." + slowLogKey, "127");
						}) //
						.verifyComplete();
			} else {
				StepVerifier.create(connection.serverCommands().getConfig(slowLogKey)) //
						.consumeNextWith(properties -> {
							assertThat(properties).containsEntry(slowLogKey, "127");
						}) //
						.verifyComplete();
			}
		} finally {
			connection.serverCommands().setConfig(slowLogKey, resetValue).block();
		}
	}

	@Test // DATAREDIS-659
	public void configResetstatShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().resetConfigStats()).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void timeShouldRespondCorrectly() {
		StepVerifier.create(connection.serverCommands().time()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void setClientNameShouldSetName() {

		// see lettuce-io/lettuce-core#563
		assumeFalse(connection instanceof LettuceReactiveRedisClusterConnection);

		StepVerifier.create(connection.serverCommands().setClientName("foo")).expectNextCount(1).verifyComplete();
		StepVerifier.create(connection.serverCommands().getClientName()).expectNext("foo").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getClientListShouldReportClient() {
		StepVerifier.create(connection.serverCommands().getClientList()).expectNextCount(1).thenCancel().verify();
	}
}
