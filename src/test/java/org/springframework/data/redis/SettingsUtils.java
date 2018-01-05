/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis;

import java.util.Properties;

import org.springframework.data.redis.connection.RedisSocketConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Utility class exposing connection settings to connect Redis instances during test execution. Settings can be adjusted
 * by overriding these in {@literal org/springframework/data/redis/test.properties}.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public abstract class SettingsUtils {

	private final static Properties DEFAULTS = new Properties();
	private static final Properties SETTINGS;

	static {
		DEFAULTS.put("host", "127.0.0.1");
		DEFAULTS.put("port", "6379");
		DEFAULTS.put("socket", "work/redis-6379.sock");

		SETTINGS = new Properties(DEFAULTS);

		try {
			SETTINGS.load(SettingsUtils.class.getResourceAsStream("/org/springframework/data/redis/test.properties"));
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot read settings");
		}
	}

	private SettingsUtils() {}

	/**
	 * @return the Redis hostname.
	 */
	public static String getHost() {
		return SETTINGS.getProperty("host");
	}

	/**
	 * @return the Redis port.
	 */
	public static int getPort() {
		return Integer.valueOf(SETTINGS.getProperty("port"));
	}

	/**
	 * @return path to the unix domain socket.
	 */
	public static String getSocket() {
		return SETTINGS.getProperty("socket");
	}

	/**
	 * Construct a new {@link RedisStandaloneConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisStandaloneConfiguration} initialized with test endpoint settings.
	 */
	public static RedisStandaloneConfiguration standaloneConfiguration() {
		return new RedisStandaloneConfiguration(getHost(), getPort());
	}

	/**
	 * Construct a new {@link RedisSocketConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisSocketConfiguration} initialized with test endpoint settings.
	 */
	public static RedisSocketConfiguration socketConfiguration() {
		return new RedisSocketConfiguration(getSocket());
	}
}
