/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Marker interface for configuration classes related to Redis connection setup. As the setup scenarios are quite
 * diverse instead of struggling with unifying those, {@link RedisConfiguration} provides means to identify
 * configurations for the individual purposes.
 *
 * @author Christoph Strobl
 * @author Luis De Bello
 * @author Vikas Garg
 * @since 2.1
 */
public interface RedisConfiguration {

	/**
	 * Get the configured database index if the current {@link RedisConfiguration} is
	 * {@link #isDatabaseIndexAware(RedisConfiguration) database aware} or evaluate and return the value of the given
	 * {@link Supplier}.
	 *
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isDatabaseIndexAware(RedisConfiguration) database aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 */
	default Integer getDatabaseOrElse(Supplier<Integer> other) {
		return getDatabaseOrElse(this, other);
	}

	/**
	 * Get the configured {@link RedisPassword} if the current {@link RedisConfiguration} is
	 * {@link #isAuthenticationAware(RedisConfiguration) password aware} or evaluate and return the value of the given
	 * {@link Supplier}.
	 *
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isAuthenticationAware(RedisConfiguration) password aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 */
	default RedisPassword getPasswordOrElse(Supplier<RedisPassword> other) {
		return getPasswordOrElse(this, other);
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link WithPassword}.
	 */
	static boolean isAuthenticationAware(@Nullable RedisConfiguration configuration) {
		return configuration instanceof WithAuthentication;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link WithDatabaseIndex}.
	 */
	static boolean isDatabaseIndexAware(@Nullable RedisConfiguration configuration) {
		return configuration instanceof WithDatabaseIndex;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link SentinelConfiguration}.
	 */
	static boolean isSentinelConfiguration(@Nullable RedisConfiguration configuration) {
		return configuration instanceof SentinelConfiguration;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link WithHostAndPort}.
	 * @since 2.1.6
	 */
	static boolean isHostAndPortAware(@Nullable RedisConfiguration configuration) {
		return configuration instanceof WithHostAndPort;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link ClusterConfiguration}.
	 */
	static boolean isClusterConfiguration(@Nullable RedisConfiguration configuration) {
		return configuration instanceof ClusterConfiguration;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link StaticMasterReplicaConfiguration}.
	 */
	static boolean isStaticMasterReplicaConfiguration(@Nullable RedisConfiguration configuration) {
		return configuration instanceof StaticMasterReplicaConfiguration;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @return {@code true} if given {@link RedisConfiguration} is instance of {@link DomainSocketConfiguration}.
	 */
	static boolean isDomainSocketConfiguration(@Nullable RedisConfiguration configuration) {
		return configuration instanceof DomainSocketConfiguration;
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isDatabaseIndexAware(RedisConfiguration) database aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 */
	static Integer getDatabaseOrElse(@Nullable RedisConfiguration configuration, Supplier<Integer> other) {

		Assert.notNull(other, "Other must not be null!");
		return isDatabaseIndexAware(configuration) ? ((WithDatabaseIndex) configuration).getDatabase() : other.get();
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isAuthenticationAware(RedisConfiguration) password aware}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 */
	@Nullable
	static String getUsernameOrElse(@Nullable RedisConfiguration configuration, Supplier<String> other) {

		Assert.notNull(other, "Other must not be null!");
		return isAuthenticationAware(configuration) ? ((WithAuthentication) configuration).getUsername() : other.get();
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isAuthenticationAware(RedisConfiguration) password aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 */
	static RedisPassword getPasswordOrElse(@Nullable RedisConfiguration configuration, Supplier<RedisPassword> other) {

		Assert.notNull(other, "Other must not be null!");
		return isAuthenticationAware(configuration) ? ((WithAuthentication) configuration).getPassword() : other.get();
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isHostAndPortAware(RedisConfiguration) port aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 * @since 2.1.6
	 */
	static int getPortOrElse(@Nullable RedisConfiguration configuration, IntSupplier other) {

		Assert.notNull(other, "Other must not be null!");
		return isHostAndPortAware(configuration) ? ((WithHostAndPort) configuration).getPort() : other.getAsInt();
	}

	/**
	 * @param configuration can be {@literal null}.
	 * @param other a {@code Supplier} whose result is returned if given {@link RedisConfiguration} is not
	 *          {@link #isHostAndPortAware(RedisConfiguration) host aware}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if {@code other} is {@literal null}.
	 * @since 2.1.6
	 */
	static String getHostOrElse(@Nullable RedisConfiguration configuration, Supplier<String> other) {

		Assert.notNull(other, "Other must not be null!");
		return isHostAndPortAware(configuration) ? ((WithHostAndPort) configuration).getHostName() : other.get();
	}

	/**
	 * {@link RedisConfiguration} part suitable for configurations that may use authentication when connecting.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.4
	 */
	interface WithAuthentication {

		/**
		 * Create and set a username with the given {@link String}. Requires Redis 6 or newer.
		 *
		 * @param username the username.
		 */
		void setUsername(@Nullable String username);

		/**
		 * Create and set a {@link RedisPassword} for given {@link String}.
		 *
		 * @param password can be {@literal null}.
		 */
		default void setPassword(@Nullable String password) {
			setPassword(RedisPassword.of(password));
		}

		/**
		 * Create and set a {@link RedisPassword} for given {@link String}.
		 *
		 * @param password can be {@literal null}.
		 */
		default void setPassword(@Nullable char[] password) {
			setPassword(RedisPassword.of(password));
		}

		/**
		 * Create and set a {@link RedisPassword} for given {@link String}.
		 *
		 * @param password must not be {@literal null} use {@link RedisPassword#none()} instead.
		 */
		void setPassword(RedisPassword password);

		/**
		 * Get the username to use when connecting.
		 *
		 * @return {@literal null} if none set.
		 */
		@Nullable
		String getUsername();

		/**
		 * Get the RedisPassword to use when connecting.
		 *
		 * @return {@link RedisPassword#none()} if none set.
		 */
		RedisPassword getPassword();
	}

	/**
	 * {@link RedisConfiguration} part suitable for configurations that may use authentication when connecting.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface WithPassword extends WithAuthentication {

	}

	/**
	 * {@link RedisConfiguration} part suitable for configurations that use a specific database.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface WithDatabaseIndex {

		/**
		 * Set the database index to use.
		 *
		 * @param dbIndex
		 */
		void setDatabase(int dbIndex);

		/**
		 * Get the database index to use.
		 *
		 * @return {@code zero} by default.
		 */
		int getDatabase();
	}

	/**
	 * {@link RedisConfiguration} part suitable for configurations that use host/port combinations for connecting.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface WithHostAndPort {

		/**
		 * Set the Redis server hostname
		 *
		 * @param hostName must not be {@literal null}.
		 */
		void setHostName(String hostName);

		/**
		 * @return never {@literal null}.
		 */
		String getHostName();

		/**
		 * Set the Redis server port.
		 *
		 * @param port
		 */
		void setPort(int port);

		/**
		 * Get the Redis server port.
		 *
		 * @return
		 */
		int getPort();
	}

	/**
	 * {@link RedisConfiguration} part suitable for configurations that use native domain sockets for connecting.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface WithDomainSocket {

		/**
		 * Set the socket.
		 *
		 * @param socket path to the Redis socket. Must not be {@literal null}.
		 */
		void setSocket(String socket);

		/**
		 * Get the domain socket.
		 *
		 * @return path to the Redis socket.
		 */
		String getSocket();
	}

	/**
	 * Configuration interface suitable for Redis Sentinel environments.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface SentinelConfiguration extends WithDatabaseIndex, WithPassword {

		/**
		 * Set the name of the master node.
		 *
		 * @param name must not be {@literal null}.
		 */
		default void setMaster(String name) {

			Assert.notNull(name, "Name of sentinel master must not be null.");

			setMaster(new SentinelMasterId(name));
		}

		/**
		 * Set the master node.
		 *
		 * @param master must not be {@literal null}.
		 */
		void setMaster(NamedNode master);

		/**
		 * Get the {@literal Sentinel} master node.
		 *
		 * @return get the master node or {@literal null} if not set.
		 */
		@Nullable
		NamedNode getMaster();

		/**
		 * Returns an {@link Collections#unmodifiableSet(Set)} of {@literal Sentinels}.
		 *
		 * @return {@link Set} of sentinels. Never {@literal null}.
		 */
		Set<RedisNode> getSentinels();

		/**
		 * Get the username used when authenticating with a Redis Server.
		 *
		 * @return can be {@literal null} if not set.
		 * @since 2.4
		 */
		@Nullable
		default String getDataNodeUsername() {
			return getUsername();
		}

		/**
		 * Get the {@link RedisPassword} used when authenticating with a Redis Server.
		 *
		 * @return never {@literal null}.
		 * @since 2.2.2
		 */
		default RedisPassword getDataNodePassword() {
			return getPassword();
		}

		/**
		 * Create and set a {@link RedisPassword} to be used when authenticating with Redis Sentinel from the given
		 * {@link String}.
		 *
		 * @param password can be {@literal null}.
		 * @since 2.2.2
		 */
		default void setSentinelPassword(@Nullable String password) {
			setSentinelPassword(RedisPassword.of(password));
		}

		/**
		 * Create and set a {@link RedisPassword} to be used when authenticating with Redis Sentinel from the given
		 * {@link Character} sequence.
		 *
		 * @param password can be {@literal null}.
		 * @since 2.2.2
		 */
		default void setSentinelPassword(@Nullable char[] password) {
			setSentinelPassword(RedisPassword.of(password));
		}

		/**
		 * Set a {@link RedisPassword} to be used when authenticating with Redis Sentinel.
		 *
		 * @param password must not be {@literal null} use {@link RedisPassword#none()} instead.
		 * @since 2.2.2
		 */
		void setSentinelPassword(RedisPassword password);

		/**
		 * Returns the {@link RedisPassword} to use when connecting to a Redis Sentinel. <br />
		 * Can be set via {@link #setSentinelPassword(RedisPassword)} or {@link RedisPassword#none()} if no password has
		 * been set.
		 *
		 * @return the {@link RedisPassword} for authenticating with Redis Sentinel.
		 * @since 2.2.2
		 */
		RedisPassword getSentinelPassword();

		/**
		 * Create and set a username with the given {@link String}. Requires Redis 6 or newer.
		 *
		 * @param sentinelUsername the username for sentinel.
		 */
		void setSentinelUsername(@Nullable String sentinelUsername);

		/**
		 * Get the username to use when connecting.
		 *
		 * @return {@literal null} if none set.
		 */
		@Nullable
		String getSentinelUsername();
	}

	/**
	 * Configuration interface suitable for Redis cluster environments.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface ClusterConfiguration extends WithPassword {

		/**
		 * Returns an {@link Collections#unmodifiableSet(Set)} of {@literal cluster nodes}.
		 *
		 * @return {@link Set} of nodes. Never {@literal null}.
		 */
		Set<RedisNode> getClusterNodes();

		/**
		 * @return max number of redirects to follow or {@literal null} if not set.
		 */
		@Nullable
		Integer getMaxRedirects();
	}

	/**
	 * Configuration interface suitable for Redis master/slave environments with fixed hosts. <br/>
	 * Redis is undergoing a nomenclature change where the term replica is used synonymously to slave.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	interface StaticMasterReplicaConfiguration extends WithDatabaseIndex, WithPassword {

		/**
		 * @return unmodifiable {@link List} of {@link RedisStandaloneConfiguration nodes}.
		 */
		List<RedisStandaloneConfiguration> getNodes();
	}

	/**
	 * Configuration interface suitable for single node redis connections using local unix domain socket.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface DomainSocketConfiguration extends WithDomainSocket, WithDatabaseIndex, WithPassword {

	}
}
