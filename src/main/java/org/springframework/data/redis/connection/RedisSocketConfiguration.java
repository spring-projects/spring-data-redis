/*
 * Copyright 2017-2022 the original author or authors.
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

import org.springframework.data.redis.connection.RedisConfiguration.DomainSocketConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Configuration class used for setting up {@link RedisConnection} via {@link RedisConnectionFactory} connecting to
 * single <a href="https://redis.io/">Redis</a> using a local unix domain socket.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class RedisSocketConfiguration implements RedisConfiguration, DomainSocketConfiguration {

	private static final String DEFAULT_SOCKET = "/tmp/redis.sock";

	private String socket = DEFAULT_SOCKET;
	private int database;
	private @Nullable String username = null;
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

		Assert.hasText(socket, "Socket path must not be null nor empty");

		this.socket = socket;
	}

	@Override
	public String getSocket() {
		return socket;
	}

	@Override
	public void setSocket(String socket) {

		Assert.hasText(socket, "Socket must not be null nor empty");
		this.socket = socket;
	}

	@Override
	public int getDatabase() {
		return database;
	}

	@Override
	public void setDatabase(int index) {

		Assert.isTrue(index >= 0, () -> String.format("Invalid DB index '%s' (a positive index required)", index));

		this.database = index;
	}

	@Override
	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	@Nullable
	@Override
	public String getUsername() {
		return this.username;
	}

	@Override
	public RedisPassword getPassword() {
		return password;
	}

	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null");

		this.password = password;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RedisSocketConfiguration)) {
			return false;
		}
		RedisSocketConfiguration that = (RedisSocketConfiguration) o;
		if (database != that.database) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(socket, that.socket)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(username, that.username)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(password, that.password);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(socket);
		result = 31 * result + database;
		result = 31 * result + ObjectUtils.nullSafeHashCode(username);
		result = 31 * result + ObjectUtils.nullSafeHashCode(password);
		return result;
	}
}
