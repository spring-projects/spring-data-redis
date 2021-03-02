/*
 * Copyright 2017-2021 the original author or authors.
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface DefaultedRedisClusterConnection extends RedisClusterConnection, DefaultedRedisConnection {

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void bgReWriteAof(RedisClusterNode node) {
		serverCommands().bgReWriteAof(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void bgSave(RedisClusterNode node) {
		serverCommands().bgSave(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long lastSave(RedisClusterNode node) {
		return serverCommands().lastSave(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void save(RedisClusterNode node) {
		serverCommands().save(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long dbSize(RedisClusterNode node) {
		return serverCommands().dbSize(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void flushDb(RedisClusterNode node) {
		serverCommands().flushDb(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void flushAll(RedisClusterNode node) {
		serverCommands().flushAll(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties info(RedisClusterNode node) {
		return serverCommands().info(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties info(RedisClusterNode node, String section) {
		return serverCommands().info(node, section);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void shutdown(RedisClusterNode node) {
		serverCommands().shutdown(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Properties getConfig(RedisClusterNode node, String pattern) {
		return serverCommands().getConfig(node, pattern);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void setConfig(RedisClusterNode node, String param, String value) {
		serverCommands().setConfig(node, param, value);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default void resetConfigStats(RedisClusterNode node) {
		serverCommands().resetConfigStats(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long time(RedisClusterNode node) {
		return serverCommands().time(node);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default Long time(RedisClusterNode node, TimeUnit timeUnit) {
		return serverCommands().time(node, timeUnit);
	}

	/** @deprecated in favor of {@link RedisConnection#serverCommands()}. */
	@Override
	@Deprecated
	default List<RedisClientInfo> getClientList(RedisClusterNode node) {
		return serverCommands().getClientList(node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#execute(String, byte[], Collection)
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	default <T> T execute(String command, byte[] key, Collection<byte[]> args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(args, "Args must not be null!");

		byte[][] commandArgs = new byte[args.size() + 1][];

		commandArgs[0] = key;
		int targetIndex = 1;

		for (byte[] binaryArgument : args) {
			commandArgs[targetIndex++] = binaryArgument;
		}

		return (T) execute(command, commandArgs);
	}
}
