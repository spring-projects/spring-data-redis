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
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import org.junit.Test;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceReactiveServerCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-659
	public void pingShouldRespondCorrectly() {
		connection.ping().as(StepVerifier::create).expectNext("PONG").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void lastSaveShouldRespondCorrectly() {
		connection.serverCommands().lastSave().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-667
	public void saveShouldRespondCorrectly() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		connection.serverCommands().save().as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void dbSizeShouldRespondCorrectly() {
		connection.serverCommands().dbSize().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushDbShouldRespondCorrectly() {

		connection.serverCommands().flushDb() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.serverCommands().dbSize().as(StepVerifier::create).expectNext(1L).verifyComplete();

		connection.serverCommands().flushDb().as(StepVerifier::create).expectNext("OK").verifyComplete();

		connection.serverCommands().dbSize().as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void flushAllShouldRespondCorrectly() {

		connection.serverCommands().flushAll() //
				.then(connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER)).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.serverCommands().dbSize().as(StepVerifier::create).expectNext(1L).verifyComplete();

		connection.serverCommands().flushAll().as(StepVerifier::create).expectNext("OK").verifyComplete();

		connection.serverCommands().dbSize().as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void infoShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			connection.serverCommands().info().as(StepVerifier::create) //
					.consumeNextWith(properties -> {

						assertThat(properties) //
								.containsKey("127.0.0.1:7379.tcp_port") //
								.containsKey("127.0.0.1:7380.tcp_port");
					}) //
					.verifyComplete();
		} else {

			connection.serverCommands().info().as(StepVerifier::create) //
					.consumeNextWith(properties -> assertThat(properties).containsKey("tcp_port")) //
					.verifyComplete();
		}
	}

	@Test // DATAREDIS-659
	public void standaloneInfoWithSectionShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			connection.serverCommands().info("server").as(StepVerifier::create) //
					.consumeNextWith(properties -> {
						assertThat(properties).isNotEmpty() //
								.containsKey("127.0.0.1:7379.tcp_port") //
								.doesNotContainKey("127.0.0.1:7379.role");
					}) //
					.verifyComplete();
		} else {

			connection.serverCommands().info("server").as(StepVerifier::create) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsKey("tcp_port").doesNotContainKey("role");
					}) //
					.verifyComplete();
		}
	}

	@Test // DATAREDIS-659
	public void getConfigShouldRespondCorrectly() {

		if (connection instanceof LettuceReactiveRedisClusterConnection) {

			connection.serverCommands().getConfig("*").as(StepVerifier::create) //
					.consumeNextWith(properties -> {
						assertThat(properties).containsEntry("127.0.0.1:7379.databases", "16");

					}) //
					.verifyComplete();
		} else {

			connection.serverCommands().getConfig("*").as(StepVerifier::create) //
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
			connection.serverCommands().setConfig(slowLogKey, "127").as(StepVerifier::create) //
					.expectNext("OK") //
					.verifyComplete();

			if (connection instanceof LettuceReactiveRedisClusterConnection) {
				connection.serverCommands().getConfig(slowLogKey).as(StepVerifier::create) //
						.consumeNextWith(properties -> {
							assertThat(properties).containsEntry("127.0.0.1:7379." + slowLogKey, "127");
						}) //
						.verifyComplete();
			} else {
				connection.serverCommands().getConfig(slowLogKey).as(StepVerifier::create) //
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
		connection.serverCommands().resetConfigStats().as(StepVerifier::create).expectNext("OK").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void timeShouldRespondCorrectly() {
		connection.serverCommands().time().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659
	public void setClientNameShouldSetName() {

		// see lettuce-io/lettuce-core#563
		assumeFalse(connection instanceof LettuceReactiveRedisClusterConnection);

		connection.serverCommands().setClientName("foo").as(StepVerifier::create).expectNextCount(1).verifyComplete();
		connection.serverCommands().getClientName().as(StepVerifier::create).expectNext("foo").verifyComplete();
	}

	@Test // DATAREDIS-659
	public void getClientListShouldReportClient() {
		connection.serverCommands().getClientList().as(StepVerifier::create).expectNextCount(1).thenCancel().verify();
	}
}
