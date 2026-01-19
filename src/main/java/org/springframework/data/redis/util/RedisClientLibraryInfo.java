/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.util;

/**
 * Utility class for building Spring Data Redis client library identification
 * strings for Redis CLIENT SETINFO.
 * <p>
 * Supports the Redis CLIENT SETINFO custom suffix pattern:
 * {@code (?<custom-name>[ -~]+) v(?<custom-version>[\d\.]+)}.
 * Multiple suffixes can be delimited with semicolons. The recommended format
 * for individual suffixes is {@code <custom-name>_v<custom-version>}.
 *
 * @author Viktoriya Kutsarova
 * @since 4.0
 */
public final class RedisClientLibraryInfo {

	/**
	 * Spring Data Redis framework name constant for CLIENT SETINFO.
	 */
	public static final String FRAMEWORK_NAME = "sdr";

	private static final String UNKNOWN_VERSION = "unknown";

	/**
	 * Get the Spring Data Redis version from the package manifest.
	 * Returns "unknown" if the version cannot be determined (for example when
	 * running from an IDE or tests without a populated Implementation-Version).
	 *
	 * @return the Spring Data Redis version, or "unknown" if not available
	 */
	public static String getVersion() {
		Package pkg = RedisClientLibraryInfo.class.getPackage();
		String version = (pkg != null ? pkg.getImplementationVersion() : null);
		return (version != null ? version : UNKNOWN_VERSION);
	}

	private RedisClientLibraryInfo() {
	}
}

