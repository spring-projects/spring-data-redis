/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Collection;
import java.util.Set;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;

/**
 * {@link RedisClusterConnection} allows sending commands to dedicated nodes within the cluster. A
 * {@link RedisClusterNode} can be obtained from {@link #clusterGetNodes()} or it can be constructed using either
 * {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} or the {@link RedisClusterNode#getId()
 * node Id}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface RedisClusterConnection extends RedisConnection, RedisClusterCommands, RedisClusterServerCommands {

	/**
	 * @param node must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see RedisConnectionCommands#ping()
	 */
	@Nullable
	String ping(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see RedisKeyCommands#keys(byte[])
	 */
	@Nullable
	Set<byte[]> keys(RedisClusterNode node, byte[] pattern);

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param node must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options);

	/**
	 * @param node must not be {@literal null}.
	 * @return {@literal null} when no keys stored at node or when used in pipeline / transaction.
	 * @see RedisKeyCommands#randomKey()
	 */
	@Nullable
	byte[] randomKey(RedisClusterNode node);

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
	@Nullable
	<T> T execute(String command, byte[] key, Collection<byte[]> args);

	/**
	 * Get {@link RedisClusterServerCommands}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	default RedisClusterServerCommands serverCommands() {
		return this;
	}
}
