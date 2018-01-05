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

import java.io.IOException;

import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.test.annotation.ProfileValueSource;

import redis.clients.jedis.Jedis;

/**
 * Implementation of {@link ProfileValueSource} that handles profile value name "redisVersion" by checking the current
 * version of Redis. 2.4.x will be returned as "2.4" and 2.6.x will be returned as "2.6". Any other version found will
 * cause an {@link UnsupportedOperationException} System property values will be returned for any key other than
 * "redisVersion"
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class RedisTestProfileValueSource implements ProfileValueSource {

	private static final String REDIS_24 = "2.4";
	private static final String REDIS_26 = "2.6";
	private static final String REDIS_28 = "2.8";
	private static final String REDIS_30 = "3.0";
	private static final String REDIS_32 = "3.2";
	private static final String REDIS_VERSION_KEY = "redisVersion";

	private static RedisTestProfileValueSource INSTANCE;

	private static final Version redisVersion;

	static {
		redisVersion = tryDetectRedisVersionOrReturn(new Version(9, 9, 9));
	}

	private static Version tryDetectRedisVersionOrReturn(Version fallbackVersion) {

		Version version = fallbackVersion;

		Jedis jedis = null;
		try {

			jedis = new Jedis(SettingsUtils.getHost(), SettingsUtils.getPort(), 100);
			String info = jedis.info();
			String versionString = (String) JedisConverters.stringToProps().convert(info).get("redis_version");

			version = RedisVersionUtils.parseVersion(versionString);
		} finally {

			if (jedis != null) {
				try {
					// force socket to be closed
					jedis.getClient().quit();
					jedis.getClient().getSocket().close();
					try {
						// need to wait a bit
						Thread.sleep(5);
					} catch (InterruptedException e) {
						// just ignore it
						Thread.interrupted();
					}
				} catch (IOException e1) {
					// ignore as well
				}
				jedis.close();
			}

		}
		return version;
	}

	public RedisTestProfileValueSource() {
		INSTANCE = this;
	}

	public String get(String key) {

		if (!REDIS_VERSION_KEY.equals(key)) {
			return System.getProperty(key);
		}

		if (redisVersion.compareTo(RedisVersionUtils.parseVersion(REDIS_32)) >= 0) {
			return REDIS_32;
		}

		if (redisVersion.compareTo(RedisVersionUtils.parseVersion(REDIS_30)) >= 0) {
			return REDIS_30;
		}

		if (redisVersion.compareTo(RedisVersionUtils.parseVersion(REDIS_28)) >= 0) {
			return REDIS_28;
		}
		if (redisVersion.compareTo(RedisVersionUtils.parseVersion(REDIS_26)) >= 0) {
			return REDIS_26;
		}
		if (redisVersion.compareTo(RedisVersionUtils.parseVersion(REDIS_24)) >= 0) {
			return REDIS_24;
		}

		throw new UnsupportedOperationException("Only Redis 2.4 and higher are supported");
	}

	public static boolean matches(String key, String value) {

		if (INSTANCE == null) {
			INSTANCE = new RedisTestProfileValueSource();
		}
		return INSTANCE.get(key) != null ? INSTANCE.get(key).equals(value) : value == null;
	}
}
