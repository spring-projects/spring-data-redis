/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Collection;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * {@link RedisClusterConnection} allows sending commands to dedicated nodes within the cluster. A
 * {@link RedisClusterNode} can be obtained from {@link #clusterGetNodes()} or it can be constructed using either
 * {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} or the {@link RedisClusterNode#getId()
 * node Id}.
 * <p>
 * {@link RedisClusterConnection Redis connections}, unlike perhaps their underlying native connection are not
 * Thread-safe and should not be shared across multiple threads.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
@NullUnmarked
public interface RedisClusterConnection
		extends RedisConnection, DefaultedRedisClusterConnection, RedisClusterCommandsProvider {

	/**
	 * @param node must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see RedisConnectionCommands#ping()
	 */
	String ping(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see RedisKeyCommands#keys(byte[])
	 */
	Set<byte @NonNull []> keys(@NonNull RedisClusterNode node, byte @NonNull [] pattern);

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param node must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte @NonNull []> scan(@NonNull RedisClusterNode node, @NonNull ScanOptions options);

	/**
	 * @param node must not be {@literal null}.
	 * @return {@literal null} when no keys stored at node or when used in pipeline / transaction.
	 * @see RedisKeyCommands#randomKey()
	 */
	byte[] randomKey(@NonNull RedisClusterNode node);

	/**
	 * Execute the given command for the {@code key} provided potentially appending args. <br />
	 * This method, other than {@link #execute(String, byte[]...)}, dispatches the command to the {@code key} serving
	 * master node.
	 *
	 * <pre>
	 * <code>
	 * // SET foo bar EX 10 NX
	 * execute("SET", "foo".getBytes(), asBinaryList("bar", "EX", 10, "NX"))
	 * </code>
	 * </pre>
	 *
	 * @param command must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return command result as delivered by the underlying Redis driver. Can be {@literal null}.
	 * @since 2.1
	 */
	default <T> T execute(@NonNull String command, byte @NonNull [] key, @NonNull Collection<byte @NonNull []> args) {

		Assert.notNull(command, "Command must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(args, "Args must not be null");

		byte[][] commandArgs = new byte[args.size() + 1][];

		commandArgs[0] = key;
		int targetIndex = 1;

		for (byte[] binaryArgument : args) {
			commandArgs[targetIndex++] = binaryArgument;
		}

		return (T) execute(command, commandArgs);
	}

}
