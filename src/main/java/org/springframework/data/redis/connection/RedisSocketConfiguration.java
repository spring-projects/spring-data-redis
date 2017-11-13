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

import org.springframework.util.Assert;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} connecting to
 * single <a href="http://redis.io/">Redis</a> using a local unix domain socket.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class RedisSocketConfiguration {

	private static final String DEFAULT_SOCKET = "/tmp/redis.sock";

	private String socket = DEFAULT_SOCKET;
	private int database;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Create a new default {@link RedisSocketConfiguration}.
	 */
	public RedisSocketConfiguration() {}

	/**
	 * Create a new {@link RedisSocketConfiguration} given {@code socket}.
	 *
	 * @param socket must not be {@literal null} or empty.
	 */
	public RedisSocketConfiguration(String socket) {

		Assert.hasText(socket, "Socket path must not be null nor empty!");

		this.socket = socket;
	}

	/**
	 * @return path to the Redis socket.
	 */
	public String getSocket() {
		return socket;
	}

	/**
	 * @param socket path to the Redis socket.
	 */
	public void setSocket(String socket) {

		Assert.hasText(socket, "Socket must not be null nor empty!");
		this.socket = socket;
	}

	/**
	 * @return the db index.
	 */
	public int getDatabase() {
		return database;
	}

	/**
	 * Sets the index of the database used by this connection factory. Default is 0.
	 *
	 * @param index database index.
	 */
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, () -> String.format("Invalid DB index '%s' (a positive index required)", index));

		this.database = index;
	}

	/**
	 * @return never {@literal null}.
	 */
	public RedisPassword getPassword() {
		return password;
	}

	/**
	 * @param password must not be {@literal null}.
	 */
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null!");

		this.password = password;
	}
}
