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
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} using connecting
 * to <a href="http://redis.io/">Redis</a>.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class RedisStandaloneConfiguration {

	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 6379;

	private String hostName = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private int database;
	private String password;

	/**
	 * Create a new default {@link RedisStandaloneConfiguration}.
	 */
	public RedisStandaloneConfiguration() {}

	/**
	 * Create a new {@link RedisStandaloneConfiguration} given {@code hostName}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 */
	public RedisStandaloneConfiguration(String hostName) {
		this(hostName, DEFAULT_PORT);
	}

	/**
	 * Create a new {@link RedisStandaloneConfiguration} given {@code hostName} and {@code port}.
	 *
	 * @param hostName must not be {@literal null} or empty.
	 * @param port a valid TCP port (1-65535).
	 */
	public RedisStandaloneConfiguration(String hostName, int port) {

		Assert.hasText(hostName, "Host name must not be null or empty!");
		Assert.isTrue(port >= 1 && port <= 65535,
				() -> String.format("Port %d must be a valid TCP port in the range between 1-65535!", port));

		this.hostName = hostName;
		this.port = port;
	}

	/**
	 * @return
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * @return
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param hostName
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	/**
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return
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

		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");

		this.database = index;
	}

	/**
	 * @return
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 */
	public void setPassword(String password) {
		this.password = password;
	}
}
