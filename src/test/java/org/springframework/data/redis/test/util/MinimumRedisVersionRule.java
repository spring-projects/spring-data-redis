/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.test.util;

import java.io.IOException;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.redis.RedisVersionUtils;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.Version;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.util.StringUtils;

import redis.clients.jedis.Jedis;

/**
 * {@link MinimumRedisVersionRule} is a custom {@link TestRule} validating {@literal redisVersion} given in
 * {@link IfProfileValue} against used redis server version.
 *
 * @author Christoph Strobl
 * @since 1.4
 */
public class MinimumRedisVersionRule implements TestRule {

	private static final String PROFILE_NAME = "redisVersion";
	private static final Version redisVersion;

	public MinimumRedisVersionRule() {}

	static {
		redisVersion = readServerVersion();
	}

	private static synchronized Version readServerVersion() {

		Version version = Version.UNKNOWN;

		Jedis jedis = null;
		try {
			jedis = new Jedis(SettingsUtils.getHost(), SettingsUtils.getPort());
			String info = jedis.info();
			String versionString = (String) JedisConverters.stringToProps().convert(info).get("redis_version");

			version = RedisVersionUtils.parseVersion(versionString);
		} finally {
			if (jedis != null) {
				try {

					jedis.disconnect();
					if (jedis.getClient().getSocket().isConnected()) {
						// force socket to be closed
						jedis.getClient().getSocket().close();
						try {
							// need to wait a bit
							Thread.sleep(5);
						} catch (InterruptedException e) {
							Thread.interrupted();
						}
					}

				} catch (IOException e1) {
					// ignore as well
				}
				jedis.close();
			}
		}

		return version;
	}

	@Override
	public Statement apply(final Statement base, Description description) {

		IfProfileValue profileValue = description.getAnnotation(IfProfileValue.class);
		final String version = (profileValue != null && profileValue.name().equals(PROFILE_NAME)) ? profileValue.value()
				: null;

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				verify(version);
				base.evaluate();
			}
		};
	}

	protected void verify(String version) throws Throwable {

		if (StringUtils.hasText(version)) {

			boolean sloppyMatch = version.endsWith("+");
			String cleanedVersionString = version.replace("+", "");
			Version expected = RedisVersionUtils.parseVersion(cleanedVersionString);

			if (sloppyMatch) {
				if (redisVersion.compareTo(expected) < 0) {
					throw new AssumptionViolatedException("Expected Redis version " + version + " but found " + redisVersion);
				}
			} else {
				if (redisVersion.compareTo(expected) == 0) {
					throw new AssumptionViolatedException("Expected Redis version " + version + " but found " + redisVersion);
				}
			}
		}
	}
}
