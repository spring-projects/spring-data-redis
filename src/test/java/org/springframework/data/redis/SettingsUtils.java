/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
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

	private static final Properties DEFAULTS = new Properties();
	private static final Properties SETTINGS;

	static {
		DEFAULTS.put("host", "127.0.0.1");
		DEFAULTS.put("port", "6379");
		DEFAULTS.put("clusterPort", "7379");
		DEFAULTS.put("sentinelPort", "26379");
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
	 * @return the Redis Cluster port.
	 */
	public static int getSentinelPort() {
		return Integer.valueOf(SETTINGS.getProperty("sentinelPort"));
	}

	/**
	 * @return the Redis Sentinel Master Id.
	 */
	public static String getSentinelMaster() {
		return "mymaster";
	}

	/**
	 * @return the Redis Cluster port.
	 */
	public static int getClusterPort() {
		return Integer.valueOf(SETTINGS.getProperty("clusterPort"));
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
	 * Construct a new {@link RedisSentinelConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisSentinelConfiguration} initialized with test endpoint settings.
	 */
	public static RedisSentinelConfiguration sentinelConfiguration() {
		return new RedisSentinelConfiguration(getSentinelMaster(),
				new HashSet<>(Arrays.asList(String.format("%s:%d", getHost(), getSentinelPort()),
						String.format("%s:%d", getHost(), getSentinelPort() + 1))));
	}

	/**
	 * Construct a new {@link RedisClusterConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisClusterConfiguration} initialized with test endpoint settings.
	 */
	public static RedisClusterConfiguration clusterConfiguration() {
		return new RedisClusterConfiguration(
				Collections.singletonList(String.format("%s:%d", getHost(), getClusterPort())));
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
