/*
 * Copyright 2017-2022 the original author or authors.
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

import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class to dispatch arbitrary Redis commands using Jedis commands.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Serhii Siryi
 * @since 2.1
 */
@SuppressWarnings({ "unchecked", "ConstantConditions" })
class JedisClientUtils {

	private static final Set<String> KNOWN_COMMANDS;

	static {
		KNOWN_COMMANDS = Arrays.stream(Protocol.Command.values()).map(Enum::name).collect(Collectors.toSet());
	}

	public static ProtocolCommand getCommand(String command) {

		if (isKnownCommand(command)) {
			return Protocol.Command.valueOf(command.trim().toUpperCase());
		}

		return () -> JedisConverters.toBytes(command);
	}

	private static boolean isKnownCommand(String command) {
		return KNOWN_COMMANDS.contains(command);
	}

}
