/*
 * Copyright 2011-2013 the original author or authors.
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

import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * @author Costin Leau
 * @author Mark Paluch
 */
public abstract class SettingsUtils {
	private final static Properties DEFAULTS = new Properties();
	private static final Properties SETTINGS;

	static {
		DEFAULTS.put("host", "127.0.0.1");
		DEFAULTS.put("port", "6379");

		SETTINGS = new Properties(DEFAULTS);

		try {
			SETTINGS.load(SettingsUtils.class.getResourceAsStream("/org/springframework/data/redis/test.properties"));
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot read settings");
		}
	}

	public static String getHost() {
		return SETTINGS.getProperty("host");
	}

	public static int getPort() {
		return Integer.valueOf(SETTINGS.getProperty("port"));
	}

	public static RedisStandaloneConfiguration standaloneConfiguration() {
		return new RedisStandaloneConfiguration(getHost(), getPort());
	}
}
