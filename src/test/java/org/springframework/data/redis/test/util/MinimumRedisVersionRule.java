/*
 * Copyright 2014 the original author or authors.
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

import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.redis.RedisVersionUtils;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.Version;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.util.StringUtils;

/**
 * {@link MinimumRedisVersionRule} is a custom {@link TestRule} validating {@literal redisVersion} given in
 * {@link IfProfileValue} against used redis server version.
 * 
 * @author Christoph Strobl
 * @since 1.4
 */
public class MinimumRedisVersionRule implements TestRule {

	private static final String PROFILE_NAME = "redisVersion";
	private Version redisVersion;

	public MinimumRedisVersionRule() {
		this.redisVersion = readServerVersion();
	}

	private Version readServerVersion() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		connectionFactory.afterPropertiesSet();
		RedisConnection connection = connectionFactory.getConnection();

		Version version = Version.UNKNOWN;

		try {

			version = RedisVersionUtils.getRedisVersion(connection);
			connection.close();
		} finally {
			connectionFactory.destroy();
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
				try {
					base.evaluate();
				} finally {
					// do noting
				}
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
