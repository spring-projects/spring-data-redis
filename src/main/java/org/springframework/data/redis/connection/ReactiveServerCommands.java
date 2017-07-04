/*
 * Copyright 2017 the original author or authors.
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

import java.util.List;
import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Redis Server commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveServerCommands {

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 *
	 * @see <a href="http://redis.io/commands/bgrewriteaof">Redis Documentation: BGREWRITEAOF</a>
	 */
	Mono<String> bgReWriteAof();

	/**
	 * Start background saving of db on server.
	 *
	 * @see <a href="http://redis.io/commands/bgsave">Redis Documentation: BGSAVE</a>
	 */
	Mono<String> bgSave();

	/**
	 * Get time of last {@link #bgSave()} operation in seconds.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/lastsave">Redis Documentation: LASTSAVE</a>
	 */
	Mono<Long> lastSave();

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @see <a href="http://redis.io/commands/save">Redis Documentation: SAVE</a>
	 */
	Mono<String> save();

	/**
	 * Get the total number of available keys in currently selected database.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/dbsize">Redis Documentation: DBSIZE</a>
	 */
	Mono<Long> dbSize();

	/**
	 * Delete all keys of the currently selected database.
	 *
	 * @see <a href="http://redis.io/commands/flushdb">Redis Documentation: FLUSHDB</a>
	 */
	Mono<String> flushDb();

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 *
	 * @see <a href="http://redis.io/commands/flushall">Redis Documentation: FLUSHALL</a>
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
	 * @return
	 * @see <a href="http://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Mono<Properties> info();

	/**
	 * Load server information for given {@code selection}.
	 *
	 * @param section
	 * @return
	 * @see <a href="http://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Mono<Properties> info(String section);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 *
	 * @param pattern
	 * @return
	 * @see <a href="http://redis.io/commands/config-get">Redis Documentation: CONFIG GET</a>
	 */
	Mono<Properties> getConfig(String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 *
	 * @param param
	 * @param value
	 * @see <a href="http://redis.io/commands/config-set">Redis Documentation: CONFIG SET</a>
	 */
	Mono<String> setConfig(String param, String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 *
	 * @see <a href="http://redis.io/commands/config-resetstat">Redis Documentation: CONFIG RESETSTAT</a>
	 */
	Mono<String> resetConfigStats();

	/**
	 * Request server timestamp using {@code TIME} command.
	 *
	 * @return current server time in milliseconds.
	 * @see <a href="http://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	Mono<Long> time();

	/**
	 * Closes a given client connection identified by {@literal host:port}.
	 *
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @see <a href="http://redis.io/commands/client-kill">Redis Documentation: CLIENT KILL</a>
	 */
	Mono<String> killClient(String host, int port);

	/**
	 * Assign given name to current connection.
	 *
	 * @param name
	 * @see <a href="http://redis.io/commands/client-setname">Redis Documentation: CLIENT SETNAME</a>
	 */
	Mono<String> setClientName(String name);

	/**
	 * Returns the name of the current connection.
	 *
	 * @see <a href="http://redis.io/commands/client-getname">Redis Documentation: CLIENT GETNAME</a>
	 * @return
	 */
	Mono<String> getClientName();

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @see <a href="http://redis.io/commands/client-list">Redis Documentation: CLIENT LIST</a>
	 */
	Flux<RedisClientInfo> getClientList();
}
