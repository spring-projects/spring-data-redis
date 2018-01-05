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
package org.springframework.data.redis.connection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Redis Server commands executed in cluster environment using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveClusterServerCommands extends ReactiveServerCommands {

	/**
	 * Start an {@literal Append Only File} rewrite process on the specific server.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} indicating command completion.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#bgReWriteAof()
	 */
	Mono<String> bgReWriteAof(RedisClusterNode node);

	/**
	 * Start background saving of db on server.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} indicating command received by server. Operation success needs to be checked via
	 *         {@link #lastSave(RedisClusterNode)}.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#bgSave()
	 */
	Mono<String> bgSave(RedisClusterNode node);

	/**
	 * Get time unix timestamp of last successful {@link #bgSave()} operation in seconds.
	 *
	 * @param node must not be {@literal null}.
	 * @return @return {@link Mono} wrapping unix timestamp.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#lastSave()
	 */
	Mono<Long> lastSave(RedisClusterNode node);

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} indicating command completion.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#save()
	 */
	Mono<String> save(RedisClusterNode node);

	/**
	 * Get the total number of available keys in currently selected database.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} wrapping number of keys.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#dbSize()
	 */
	Mono<Long> dbSize(RedisClusterNode node);

	/**
	 * Delete all keys of the currently selected database.
	 *
	 * @param node must not be {@literal null}. {@link Mono} indicating command completion.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#flushDb()
	 */
	Mono<String> flushDb(RedisClusterNode node);

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} indicating command completion.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#flushAll()
	 */
	Mono<String> flushAll(RedisClusterNode node);

	/**
	 * Load {@literal default} server information like
	 * <ul>
	 * <li>memory</li>
	 * <li>cpu utilization</li>
	 * <li>replication</li>
	 * </ul>
	 * <p>
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} wrapping server information.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#info()
	 */
	Mono<Properties> info(RedisClusterNode node);

	/**
	 * Load server information for given {@code selection}.
	 *
	 * @param node must not be {@literal null}.
	 * @param section must not be {@literal null} nor {@literal empty}.
	 * @return {@link Mono} wrapping server information of given {@code section}.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @throws IllegalArgumentException when section is {@literal null} or {@literal empty}.
	 * @see RedisServerCommands#info(String)
	 */
	Mono<Properties> info(RedisClusterNode node, String section);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 *
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return {@link Mono} wrapping configuration parameters matching given {@code pattern}.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @throws IllegalArgumentException when {@code pattern} is {@literal null} or {@literal empty}.
	 * @see RedisServerCommands#getConfig(String)
	 */
	Mono<Properties> getConfig(RedisClusterNode node, String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 *
	 * @param node must not be {@literal null}.
	 * @param param must not be {@literal null} nor {@literal empty}.
	 * @param value must not be {@literal null} nor {@literal empty}.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @throws IllegalArgumentException when {@code pattern} / {@code value} is {@literal null} or {@literal empty}.
	 * @see RedisServerCommands#setConfig(String, String)
	 */
	Mono<String> setConfig(RedisClusterNode node, String param, String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} indicating command completion.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#resetConfigStats()
	 */
	Mono<String> resetConfigStats(RedisClusterNode node);

	/**
	 * Request server timestamp using {@code TIME} command.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Mono} wrapping current server time in milliseconds.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#time()
	 */
	Mono<Long> time(RedisClusterNode node);

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@link Flux} emitting {@link RedisClientInfo} objects.
	 * @throws IllegalArgumentException when {@code node} is {@literal null}.
	 * @see RedisServerCommands#getClientList()
	 */
	Flux<RedisClientInfo> getClientList(RedisClusterNode node);
}
