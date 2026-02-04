/*
 * Copyright 2026-present the original author or authors.
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

import org.junit.jupiter.api.Test;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link JedisClientUtils}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientUtilsUnitTests {

	@Test // GH-XXXX
	void getCommandShouldReturnProtocolCommandForKnownCommand() {

		ProtocolCommand command = JedisClientUtils.getCommand("GET");

		assertThat(command).isEqualTo(Protocol.Command.GET);
	}

	@Test // GH-XXXX
	void getCommandShouldReturnCustomCommandForLowerCaseCommand() {

		ProtocolCommand command = JedisClientUtils.getCommand("get");

		assertThat(command).isNotNull();
		assertThat(command.getRaw()).isEqualTo("get".getBytes());
	}

	@Test // GH-XXXX
	void getCommandShouldReturnCustomCommandForMixedCaseCommand() {

		ProtocolCommand command = JedisClientUtils.getCommand("GeT");

		assertThat(command).isNotNull();
		assertThat(command.getRaw()).isEqualTo("GeT".getBytes());
	}

	@Test // GH-XXXX
	void getCommandShouldReturnCustomCommandForCommandWithWhitespace() {

		ProtocolCommand command = JedisClientUtils.getCommand("  SET  ");

		assertThat(command).isNotNull();
		assertThat(command.getRaw()).isEqualTo("  SET  ".getBytes());
	}

	@Test // GH-XXXX
	void getCommandShouldReturnCustomCommandForUnknownCommand() {

		ProtocolCommand command = JedisClientUtils.getCommand("CUSTOM_COMMAND");

		assertThat(command).isNotNull();
		assertThat(command.getRaw()).isEqualTo("CUSTOM_COMMAND".getBytes());
	}

	@Test // GH-XXXX
	void getCommandShouldHandleMultipleKnownCommands() {

		assertThat(JedisClientUtils.getCommand("GET")).isEqualTo(Protocol.Command.GET);
		assertThat(JedisClientUtils.getCommand("SET")).isEqualTo(Protocol.Command.SET);
		assertThat(JedisClientUtils.getCommand("DEL")).isEqualTo(Protocol.Command.DEL);
		assertThat(JedisClientUtils.getCommand("HGET")).isEqualTo(Protocol.Command.HGET);
		assertThat(JedisClientUtils.getCommand("LPUSH")).isEqualTo(Protocol.Command.LPUSH);
	}

	@Test // GH-XXXX
	void getCommandShouldHandleMultipleUnknownCommands() {

		ProtocolCommand cmd1 = JedisClientUtils.getCommand("UNKNOWN1");
		ProtocolCommand cmd2 = JedisClientUtils.getCommand("UNKNOWN2");

		assertThat(cmd1.getRaw()).isEqualTo("UNKNOWN1".getBytes());
		assertThat(cmd2.getRaw()).isEqualTo("UNKNOWN2".getBytes());
	}
}
