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

import org.springframework.data.redis.connection.RedisConnection;

/**
 * Utilities for examining the Redis version
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
public abstract class RedisVersionUtils {

	public static Version getRedisVersion(RedisConnection connection) {
		return parseVersion((String) connection.info().get("redis_version"));
	}

	public static boolean atLeast(String version, RedisConnection connection) {
		return getRedisVersion(connection).compareTo(parseVersion(version)) >= 0;
	}

	public static boolean atMost(String version, RedisConnection connection) {
		return getRedisVersion(connection).compareTo(parseVersion(version)) <= 0;
	}

	public static Version parseVersion(String version) {

		Version resolvedVersion = VersionParser.parseVersion(version);
		if (Version.UNKNOWN.equals(resolvedVersion)) {
			throw new IllegalArgumentException("Specified version cannot be parsed");
		}
		return resolvedVersion;
	}
}
