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

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Transaction;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.util.ReflectionUtils;

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

	private static final Field TRANSACTION;
	private static final Set<String> KNOWN_COMMANDS;

	static {

		TRANSACTION = ReflectionUtils.findField(Jedis.class, "transaction");
		ReflectionUtils.makeAccessible(TRANSACTION);

		KNOWN_COMMANDS = Arrays.stream(Protocol.Command.values()).map(Enum::name).collect(Collectors.toSet());
	}

	/**
	 * Execute an arbitrary on the supplied {@link Jedis} instance.
	 *
	 * @param command the command.
	 * @param keys must not be {@literal null}, may be empty.
	 * @param args must not be {@literal null}, may be empty.
	 * @param jedis must not be {@literal null}.
	 * @return the response, can be {@literal null}.
	 */
	static <T> T execute(String command, byte[][] keys, byte[][] args, Supplier<Jedis> jedis) {
		return execute(command, keys, args, jedis, it -> (T) it.getOne());
	}

	/**
	 * Execute an arbitrary on the supplied {@link Jedis} instance.
	 *
	 * @param command the command.
	 * @param keys must not be {@literal null}, may be empty.
	 * @param args must not be {@literal null}, may be empty.
	 * @param jedis must not be {@literal null}.
	 * @param responseMapper must not be {@literal null}.
	 * @return the response, can be {@literal null}.
	 * @since 2.1
	 */
	static <T> T execute(String command, byte[][] keys, byte[][] args, Supplier<Jedis> jedis,
			Function<Connection, T> responseMapper) {

		byte[][] commandArgs = getCommandArguments(keys, args);

		Connection Connection = sendCommand(command, commandArgs, jedis.get());

		return responseMapper.apply(Connection);
	}

	/**
	 * Send a Redis command and retrieve the {@link Connection} for response retrieval.
	 *
	 * @param command the command.
	 * @param args must not be {@literal null}, may be empty.
	 * @param jedis must not be {@literal null}.
	 * @return the {@link Connection} instance used to send the command.
	 */
	static Connection sendCommand(String command, byte[][] args, Jedis jedis) {

		Connection Connection = jedis.getConnection();

		sendCommand(Connection, command, args);

		return Connection;
	}

	private static void sendCommand(Connection Connection, String command, byte[][] args) {

		if (isKnownCommand(command)) {
			Connection.sendCommand(Protocol.Command.valueOf(command.trim().toUpperCase()), args);
		} else {
			Connection.sendCommand(() -> command.trim().toUpperCase().getBytes(StandardCharsets.UTF_8), args);
		}
	}

	private static boolean isKnownCommand(String command) {
		return KNOWN_COMMANDS.contains(command);
	}

	private static byte[][] getCommandArguments(byte[][] keys, byte[][] args) {

		if (keys.length == 0) {
			return args;
		}

		if (args.length == 0) {
			return keys;
		}

		byte[][] commandArgs = new byte[keys.length + args.length][];

		System.arraycopy(keys, 0, commandArgs, 0, keys.length);
		System.arraycopy(args, 0, commandArgs, keys.length, args.length);

		return commandArgs;
	}

	/**
	 * @param jedis the Connection instance.
	 * @return {@literal true} if the connection has entered {@literal MULTI} state.
	 */
	static boolean isInMulti(Jedis jedis) {

		Object field = ReflectionUtils.getField(TRANSACTION, jedis);

		return field instanceof Transaction;
	}
}
