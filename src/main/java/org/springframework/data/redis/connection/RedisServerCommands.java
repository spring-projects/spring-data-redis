/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.List;
import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Server-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public interface RedisServerCommands {

	public enum ShutdownOption {
		SAVE, NOSAVE;
	}

	/**
	 * @since 1.7
	 */
	public enum MigrateOption {
		COPY, REPLACE
	}

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 * 
	 * @deprecated As of 1.3, use {@link #bgReWriteAof}.
	 * @see <a href="http://redis.io/commands/bgrewriteaof">Redis Documentation: BGREWRITEAOF</a>
	 */
	@Deprecated
	void bgWriteAof();

	/**
	 * Start an {@literal Append Only File} rewrite process on server.
	 *
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/bgrewriteaof">Redis Documentation: BGREWRITEAOF</a>
	 */
	void bgReWriteAof();

	/**
	 * Start background saving of db on server.
	 *
	 * @see <a href="http://redis.io/commands/bgsave">Redis Documentation: BGSAVE</a>
	 */
	void bgSave();

	/**
	 * Get time of last {@link #bgSave()} operation in seconds.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/lastsave">Redis Documentation: LASTSAVE</a>
	 */
	Long lastSave();

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @see <a href="http://redis.io/commands/save">Redis Documentation: SAVE</a>
	 */
	void save();

	/**
	 * Get the total number of available keys in currently selected database.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/dbsize">Redis Documentation: DBSIZE</a>
	 */
	Long dbSize();

	/**
	 * Delete all keys of the currently selected database.
	 *
	 * @see <a href="http://redis.io/commands/flushdb">Redis Documentation: FLUSHDB</a>
	 */
	void flushDb();

	/**
	 * Delete all <b>all keys</b> from <b>all databases</b>.
	 *
	 * @see <a href="http://redis.io/commands/flushall">Redis Documentation: FLUSHALL</a>
	 */
	void flushAll();

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
	Properties info();

	/**
	 * Load server information for given {@code selection}.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/info">Redis Documentation: INFO</a>
	 */
	Properties info(String section);

	/**
	 * Shutdown server.
	 *
	 * @see <a href="http://redis.io/commands/shutdown">Redis Documentation: SHUTDOWN</a>
	 */
	void shutdown();

	/**
	 * Shutdown server.
	 *
	 * @see <a href="http://redis.io/commands/shutdown">Redis Documentation: SHUTDOWN</a>
	 * @since 1.3
	 */
	void shutdown(ShutdownOption option);

	/**
	 * Load configuration parameters for given {@code pattern} from server.
	 *
	 * @param pattern
	 * @return
	 * @see <a href="http://redis.io/commands/config-get">Redis Documentation: CONFIG GET</a>
	 */
	List<String> getConfig(String pattern);

	/**
	 * Set server configuration for {@code param} to {@code value}.
	 *
	 * @param param
	 * @param value
	 * @see <a href="http://redis.io/commands/config-set">Redis Documentation: CONFIG SET</a>
	 */
	void setConfig(String param, String value);

	/**
	 * Reset statistic counters on server. <br>
	 * Counters can be retrieved using {@link #info()}.
	 *
	 * @see <a href="http://redis.io/commands/config-resetstat">Redis Documentation: CONFIG RESETSTAT</a>
	 */
	void resetConfigStats();

	/**
	 * Request server timestamp using {@code TIME} command.
	 * 
	 * @return current server time in milliseconds.
	 * @since 1.1
	 * @see <a href="http://redis.io/commands/time">Redis Documentation: TIME</a>
	 */
	Long time();

	/**
	 * Closes a given client connection identified by {@literal host:port}.
	 * 
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/client-kill">Redis Documentation: CLIENT KILL</a>
	 */
	void killClient(String host, int port);

	/**
	 * Assign given name to current connection.
	 * 
	 * @param name
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/client-setname">Redis Documentation: CLIENT SETNAME</a>
	 */
	void setClientName(byte[] name);

	/**
	 * Returns the name of the current connection.
	 *
	 * @see <a href="http://redis.io/commands/client-getname">Redis Documentation: CLIENT GETNAME</a>
	 * @return
	 * @since 1.3
	 */
	String getClientName();

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/client-list">Redis Documentation: CLIENT LIST</a>
	 */
	List<RedisClientInfo> getClientList();

	/**
	 * Change redis replication setting to new master.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/slaveof">Redis Documentation: SLAVEOF</a>
	 */
	void slaveOf(String host, int port);

	/**
	 * Change server into master.
	 *
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/slaveof">Redis Documentation: SLAVEOF</a>
	 */
	void slaveOfNoOne();

	/**
	 * Atomically transfer a key from a source Redis instance to a destination Redis instance. On success the key is
	 * deleted from the original instance and is guaranteed to exist in the target instance.
	 * 
	 * @param key must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 * @param dbIndex
	 * @param option can be {@literal null}. Defaulted to {@link MigrateOption#COPY}.
	 * @since 1.7
	 * @see <a href="http://redis.io/commands/migrate">Redis Documentation: MIGRATE</a>
	 */
	void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option);

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
	 * @see <a href="http://redis.io/commands/migrate">Redis Documentation: MIGRATE</a>
	 */
	void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout);
}
