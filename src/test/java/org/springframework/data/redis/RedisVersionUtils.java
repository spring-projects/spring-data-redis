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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.redis.connection.RedisConnection;

/**
 * Utilities for examining the Redis version
 *
 * @author Jennifer Hickey
 *
 */
public abstract class RedisVersionUtils {

	private static final Pattern VERSION_MATCHER = Pattern
			.compile("([0-9]+)\\.([0-9]+)(\\.([0-9]+))?");

	public static Version getRedisVersion(RedisConnection connection) {
		return parseVersion((String) connection.info().get("redis_version"));
	}

	public static boolean atLeast(String version, RedisConnection connection) {
		return getRedisVersion(connection).compareTo(parseVersion(version)) >= 0;
	}

	public static boolean atMost(String version, RedisConnection connection) {
		return getRedisVersion(connection).compareTo(parseVersion(version)) <= 0;
	}

	private static Version parseVersion(String version) {
		Matcher matcher = VERSION_MATCHER.matcher(version);
		if (matcher.matches()) {
			String major = matcher.group(1);
			String minor = matcher.group(2);
			String patch = matcher.group(4);
			return new Version(Integer.parseInt(major), Integer.parseInt(minor),
					Integer.parseInt(patch));
		}
		throw new IllegalArgumentException("Specified version cannot be parsed");
	}
}
