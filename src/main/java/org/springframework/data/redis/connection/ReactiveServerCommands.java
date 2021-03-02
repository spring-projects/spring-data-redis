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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Redis Server commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveServerCommands {

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 *
	 * @return {@link Mono} indicating command completion.
	 * @see <a href="https://redis.io/commands/bgrewriteaof">Redis Documentation: BGREWRITEAOF</a>
	 */
	Mono<String> bgReWriteAof();

	/**
	 * Start background saving of db on server.
	 *
	 * @return {@link Mono} indicating command received by server. Operation success needs to be checked via
	 *         {@link #lastSave()}.
	 * @see <a href="https://redis.io/commands/bgsave">Redis Documentation: BGSAVE</a>
	 */
	Mono<String> bgSave();

	/**
	 * Get time unix timestamp of last successful {@link #bgSave()} operation in seconds.
	 *
	 * @return {@link Mono} wrapping unix timestamp.
	 * @see <a href="https://redis.io/commands/lastsave">Redis Documentation: LASTSAVE</a>
	 */
	Mono<Long> lastSave();

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @return {@link Mono} indicating command completion.
	 * @see <a href="https://redis.io/commands/save">Redis Documentation: SAVE</a>
	 */
	Mono<String> save();

	/**
	 * Get the total number of available keys in currently selected database.
	 *
	 * @return {@link Mono} wrapping number of keys.
	 * @see <a href="https://redis.io/commands/dbsize">Redis Documentation: DBSIZE</a>
	 */
	Mono<Long> dbSize();

	/**
	 * Delete all keys of the currently selected database.
	 *
	 * @return {@link Mono} indicating command completion.
	 * @see <a href="https://redis.io/commands/flushdb">Redis Documentation: FLUSHDB</a>
	 */
	Mono<String> flushDb();

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 *
	 * @return {@link Mono} indicating command completion.
	 * @see <a href="https://redis.io/commands/flushall">Redis Documentation: FLUSHALL</a>
	 */
	Mono<String> flushAll();

	/**
	 * Load {@literal default} server information like
	 * <ul>
	 * <li>memory</li>
	 * <li>cpu utilization</li>
	 * <li>replication</li>
	 * </ul>
	 * <p>
	 *
	 * @return {@link Mono} wrapping server information.
	 * @see <a href="https://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Mono<Properties> info();

	/**
	 * Load server information for given {@code selection}.
	 *
	 * @param section must not be {@literal null} nor {@literal empty}.
	 * @return {@link Mono} wrapping server information of given {@code section}.
	 * @throws IllegalArgumentException when section is {@literal null} or {@literal empty}.
	 * @see <a href="https://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Mono<Properties> info(String section);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return {@link Mono} wrapping configuration parameters matching given {@code pattern}.
	 * @throws IllegalArgumentException when {@code pattern} is {@literal null} or {@literal empty}.
	 * @see <a href="https://redis.io/commands/config-get">Redis Documentation: CONFIG GET</a>
	 */
	Mono<Properties> getConfig(String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 *
	 * @param param must not be {@literal null} nor {@literal empty}.
	 * @param value must not be {@literal null} nor {@literal empty}.
	 * @throws IllegalArgumentException when {@code pattern} / {@code value} is {@literal null} or {@literal empty}.
	 * @see <a href="https://redis.io/commands/config-set">Redis Documentation: CONFIG SET</a>
	 */
	Mono<String> setConfig(String param, String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 *
	 * @return {@link Mono} indicating command completion.
	 * @see <a href="https://redis.io/commands/config-resetstat">Redis Documentation: CONFIG RESETSTAT</a>
	 */
	Mono<String> resetConfigStats();

	/**
	 * Request server timestamp using {@code TIME} command in {@link TimeUnit#MILLISECONDS}.
	 *
	 * @return {@link Mono} wrapping current server time in milliseconds.
	 * @see <a href="https://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	default Mono<Long> time() {
		return time(TimeUnit.MILLISECONDS);
	}

	/**
	 * Request server timestamp using {@code TIME} command.
	 *
	 * @param timeUnit target unit.
	 * @return {@link Mono} wrapping current server time in {@link TimeUnit}.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	Mono<Long> time(TimeUnit timeUnit);

	/**
	 * Closes a given client connection identified by {@literal host:port}.
	 *
	 * @param host of connection to close. Must not be {@literal null} nor {@literal empty}.
	 * @param port of connection to close
	 * @return {@link Mono} wrapping {@link String} representation of the command result.
	 * @throws IllegalArgumentException if {@code host} is {@literal null} or {@literal empty}.
	 * @see <a href="https://redis.io/commands/client-kill">Redis Documentation: CLIENT KILL</a>
	 */
	Mono<String> killClient(String host, int port);

	/**
	 * Assign given name to current connection.
	 *
	 * @param name must not be {@literal null} nor {@literal empty}.
	 * @throws IllegalArgumentException when {@code name} is {@literal null} or {@literal empty}.
	 * @see <a href="https://redis.io/commands/client-setname">Redis Documentation: CLIENT SETNAME</a>
	 */
	Mono<String> setClientName(String name);

	/**
	 * Returns the name of the current connection.
	 *
	 * @return {@link Mono} wrapping the connection name.
	 * @see <a href="https://redis.io/commands/client-getname">Redis Documentation: CLIENT GETNAME</a>
	 */
	Mono<String> getClientName();

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link Flux} emitting {@link RedisClientInfo} objects.
	 * @see <a href="https://redis.io/commands/client-list">Redis Documentation: CLIENT LIST</a>
	 */
	Flux<RedisClientInfo> getClientList();
}
