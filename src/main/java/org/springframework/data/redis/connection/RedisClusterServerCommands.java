/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 2.0
 */
@NullUnmarked
public interface RedisClusterServerCommands extends RedisServerCommands {

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#bgReWriteAof()
	 */
	void bgReWriteAof(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#bgSave()
	 */
	void bgSave(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#lastSave()
	 */
	Long lastSave(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#save()
	 */
	void save(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#dbSize()
	 */
	Long dbSize(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#flushDb()
	 */
	void flushDb(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param option
	 * @see RedisServerCommands#flushDb(FlushOption)
	 * @since 2.7
	 */
	void flushDb(@NonNull RedisClusterNode node, @NonNull FlushOption option);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#flushAll()
	 */
	void flushAll(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param option
	 * @see RedisServerCommands#flushAll(FlushOption)
	 * @since 2.7
	 */
	void flushAll(@NonNull RedisClusterNode node, @NonNull FlushOption option);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#info()
	 */
	Properties info(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param section
	 * @return
	 * @see RedisServerCommands#info(String)
	 */
	Properties info(@NonNull RedisClusterNode node, @NonNull String section);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#shutdown()
	 */
	void shutdown(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param pattern
	 * @return
	 * @see RedisServerCommands#getConfig(String)
	 */
	Properties getConfig(@NonNull RedisClusterNode node, @NonNull String pattern);

	/**
	 * @param node must not be {@literal null}.
	 * @param param
	 * @param value
	 * @see RedisServerCommands#setConfig(String, String)
	 */
	void setConfig(@NonNull RedisClusterNode node, @NonNull String param, @NonNull String value);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#resetConfigStats()
	 */
	void resetConfigStats(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @see RedisServerCommands#rewriteConfig()
	 * @since 2.5
	 */
	void rewriteConfig(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#time()
	 */
	default Long time(@NonNull RedisClusterNode node) {
		return time(node, TimeUnit.MILLISECONDS);
	}

	/**
	 * @param node must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return
	 * @since 2.5
	 * @see RedisServerCommands#time(TimeUnit)
	 */
	Long time(@NonNull RedisClusterNode node, @NonNull TimeUnit timeUnit);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisServerCommands#getClientList()
	 */
	List<@NonNull RedisClientInfo> getClientList(@NonNull RedisClusterNode node);
}
