/*
 * Copyright 2011-2025 the original author or authors.
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
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Server-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisServerCommands {

	enum ShutdownOption {
		SAVE, NOSAVE;
	}

	/**
	 * @since 1.7
	 */
	enum MigrateOption {
		COPY, REPLACE
	}

	/**
	 * @since 2.7
	 */
	enum FlushOption {
		SYNC, ASYNC
	}

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 *
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/bgrewriteaof">Redis Documentation: BGREWRITEAOF</a>
	 */
	void bgReWriteAof();

	/**
	 * Start background saving of db on server.
	 *
	 * @see <a href="https://redis.io/commands/bgsave">Redis Documentation: BGSAVE</a>
	 */
	void bgSave();

	/**
	 * Get time of last {@link #bgSave()} operation in seconds.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lastsave">Redis Documentation: LASTSAVE</a>
	 */
	Long lastSave();

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @see <a href="https://redis.io/commands/save">Redis Documentation: SAVE</a>
	 */
	void save();

	/**
	 * Get the total number of available keys in currently selected database.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/dbsize">Redis Documentation: DBSIZE</a>
	 */
	Long dbSize();

	/**
	 * Delete all keys of the currently selected database.
	 *
	 * @see <a href="https://redis.io/commands/flushdb">Redis Documentation: FLUSHDB</a>
	 */
	void flushDb();

	/**
	 * Delete all keys of the currently selected database using the specified {@link FlushOption}.
	 *
	 * @param option
	 * @see <a href="https://redis.io/commands/flushdb">Redis Documentation: FLUSHDB</a>
	 * @since 2.7
	 */
	void flushDb(@NonNull FlushOption option);

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 *
	 * @see <a href="https://redis.io/commands/flushall">Redis Documentation: FLUSHALL</a>
	 */
	void flushAll();

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b> using the specified {@link FlushOption}.
	 *
	 * @param option
	 * @see <a href="https://redis.io/commands/flushall">Redis Documentation: FLUSHALL</a>
	 * @since 2.7
	 */
	void flushAll(@NonNull FlushOption option);

	/**
	 * Load {@literal default} server information like
	 * <ul>
	 * <li>memory</li>
	 * <li>cpu utilization</li>
	 * <li>replication</li>
	 * </ul>
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Properties info();

	/**
	 * Load server information for given {@code selection}.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Properties info(@NonNull String section);

	/**
	 * Shutdown server.
	 *
	 * @see <a href="https://redis.io/commands/shutdown">Redis Documentation: SHUTDOWN</a>
	 */
	void shutdown();

	/**
	 * Shutdown server.
	 *
	 * @param option shutdown options if applicable.
	 * @see <a href="https://redis.io/commands/shutdown">Redis Documentation: SHUTDOWN</a>
	 * @since 1.3
	 */
	void shutdown(@Nullable ShutdownOption option);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/config-get">Redis Documentation: CONFIG GET</a>
	 */
	Properties getConfig(@NonNull String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 *
	 * @param param must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/config-set">Redis Documentation: CONFIG SET</a>
	 */
	void setConfig(@NonNull String param, @NonNull String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 *
	 * @see <a href="https://redis.io/commands/config-resetstat">Redis Documentation: CONFIG RESETSTAT</a>
	 */
	void resetConfigStats();

	/**
	 * Rewrites the {@code redis.conf} file.
	 *
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/config-rewrite">Redis Documentation: CONFIG REWRITE</a>
	 */
	void rewriteConfig();

	/**
	 * Request server timestamp using {@code TIME} command in {@link TimeUnit#MILLISECONDS}.
	 *
	 * @return current server time in milliseconds or {@literal null} when used in pipeline / transaction.
	 * @since 1.1
	 * @see <a href="https://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	default Long time() {
		return time(TimeUnit.MILLISECONDS);
	}

	/**
	 * Request server timestamp using {@code TIME} command.
	 *
	 * @param timeUnit target unit.
	 * @return current server time in {@link TimeUnit} or {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	Long time(@NonNull TimeUnit timeUnit);

	/**
	 * Closes a given client connection identified by {@literal host:port}.
	 *
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/client-kill">Redis Documentation: CLIENT KILL</a>
	 */
	void killClient(@NonNull String host, int port);

	/**
	 * Assign given name to current connection.
	 *
	 * @param name
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/client-setname">Redis Documentation: CLIENT SETNAME</a>
	 */
	void setClientName(byte @NonNull [] name);

	/**
	 * Returns the name of the current connection.
	 *
	 * @see <a href="https://redis.io/commands/client-getname">Redis Documentation: CLIENT GETNAME</a>
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 */
	String getClientName();

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects or {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/client-list">Redis Documentation: CLIENT LIST</a>
	 */
	List<@NonNull RedisClientInfo> getClientList();

	/**
	 * Change redis replication setting to new master.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/replicaof">Redis Documentation: REPLICAOF</a>
	 */
	void replicaOf(@NonNull String host, int port);

	/**
	 * Change server into master.
	 *
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/replicaof">Redis Documentation: REPLICAOF</a>
	 */
	void replicaOfNoOne();

	/**
	 * Atomically transfer a key from a source Redis instance to a destination Redis instance. On success the key is
	 * deleted from the original instance and is guaranteed to exist in the target instance.
	 *
	 * @param key must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 * @param dbIndex
	 * @param option can be {@literal null}. Defaulted to {@link MigrateOption#COPY}.
	 * @since 1.7
	 * @see <a href="https://redis.io/commands/migrate">Redis Documentation: MIGRATE</a>
	 */
	void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option);

	/**
	 * Atomically transfer a key from a source Redis instance to a destination Redis instance. On success the key is
	 * deleted from the original instance and is guaranteed to exist in the target instance.
	 *
	 * @param key must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 * @param dbIndex
	 * @param option can be {@literal null}. Defaulted to {@link MigrateOption#COPY}.
	 * @param timeout
	 * @since 1.7
	 * @see <a href="https://redis.io/commands/migrate">Redis Documentation: MIGRATE</a>
	 */
	void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option,
			long timeout);

}
