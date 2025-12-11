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

import org.jspecify.annotations.Nullable;

import org.springframework.util.StringUtils;

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
	 * Lettuce driver name constant for CLIENT SETINFO.
	 */
	public static final String DRIVER_LETTUCE = "lettuce";

	/**
	 * Spring Data Redis framework name constant for CLIENT SETINFO.
	 */
	public static final String FRAMEWORK_NAME = "spring-data-redis";

	private static final String SUFFIX_DELIMITER = ";";

	private static final String VERSION_SEPARATOR = "_v";

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

	/**
	 * Build a library name suffix for CLIENT SETINFO in the format:
	 * {@code spring-data-redis_v<version>}
	 * <p>
	 * Note: The underscore before 'v' follows the Redis CLIENT SETINFO pattern recommendation.
	 *
	 * @return the library name suffix
	 */
	public static String getLibNameSuffix() {
		return FRAMEWORK_NAME + VERSION_SEPARATOR + getVersion();
	}

	/**
	 * Build a library name suffix with additional framework suffix(es) for CLIENT SETINFO.
	 * This allows multiple higher-level frameworks to identify themselves in a chain.
	 * <p>
	 * The {@code additionalSuffix} parameter should already be formatted according to the pattern
	 * and can contain multiple frameworks separated by semicolons.
	 * <p>
	 * Format: {@code <additionalSuffix>;spring-data-redis_v<version>}
	 * <p>
	 * Example with multiple frameworks:
	 * <pre>
	 * String suffix = RedisClientInfo.getLibNameSuffix(
	 *     "spring-security_v6.0.0;spring-session-data-redis_v3.0.0"
	 * );
	 * // Returns: "spring-security_v6.0.0;spring-session-data-redis_v3.0.0;spring-data-redis_v4.0.0"
	 * </pre>
	 *
	 * @param additionalSuffix pre-formatted suffix string containing one or more framework identifiers,
	 *                        already in the format "name_version" and separated by semicolons if multiple
	 * @return the combined library name suffix with all frameworks and Spring Data Redis info
	 */
	public static String getLibNameSuffix(@Nullable String additionalSuffix) {
		if (!StringUtils.hasText(additionalSuffix)) {
			return getLibNameSuffix();
		}
		return additionalSuffix + SUFFIX_DELIMITER + getLibNameSuffix();
	}

	/**
	 * Build a complete library name for CLIENT SETINFO by wrapping the suffix with the core driver name.
	 * This allows multiple higher-level frameworks to identify themselves in a chain.
	 * <p>
	 * Format: {@code <driverName>(<additionalSuffix>;spring-data-redis_v<version>)}
	 * <p>
	 * Example:
	 * <pre>
	 * String libName = RedisClientInfo.getLibName("lettuce",
	 *     "spring-security_v6.0.0;spring-session-data-redis_v3.0.0");
	 * // Returns: "lettuce(spring-security_v6.0.0;spring-session-data-redis_v3.0.0;spring-data-redis_v4.0.0)"
	 * </pre>
	 *
	 * @param driverName the core Redis driver name (e.g., "lettuce", "jedis")
	 * @param additionalSuffix pre-formatted suffix string containing one or more framework identifiers,
	 *                        already in the format "name_version" and separated by semicolons if multiple
	 * @return the complete library name in the format "driverName(additionalSuffix;spring-data-redis_version)"
	 */
	public static String getLibName(String driverName, @Nullable String additionalSuffix) {
		return driverName + "(" + getLibNameSuffix(additionalSuffix) + ")";
	}
}

