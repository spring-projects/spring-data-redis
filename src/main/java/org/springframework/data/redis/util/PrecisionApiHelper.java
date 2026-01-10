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

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;

/**
 * Helper utility for executing precision millisecond-based Redis commands with automatic fallback
 * to legacy second-based commands when precision APIs are not supported.
 * <p>
 * This utility is used internally to handle compatibility across different Redis versions and drivers.
 * Redis 2.6+ introduced millisecond-precision commands (pExpire, pExpireAt, pTtl) as alternatives
 * to the original second-precision commands (expire, expireAt, ttl).
 *
 * @author Youngsuk Kim
 * @since 4.0.1
 */
public final class PrecisionApiHelper {

	private static final Logger logger = LoggerFactory.getLogger(PrecisionApiHelper.class);

	private PrecisionApiHelper() {}

	/**
	 * Enum representing Redis precision commands for type-safe operation naming.
	 *
	 * @since 4.0.1
	 */
	public enum PrecisionCommand {

		/** Precision millisecond-based expiration command (PEXPIRE) */
		PEXPIRE("pExpire"),

		/** Precision millisecond-based expiration at timestamp command (PEXPIREAT) */
		PEXPIREAT("pExpireAt"),

		/** Precision millisecond-based time-to-live command (PTTL) */
		PTTL("pTtl");

		private final String commandName;

		PrecisionCommand(String commandName) {
			this.commandName = commandName;
		}

    @Override
		public String toString() {
			return commandName;
		}
	}

	/**
	 * Attempts to execute a precision millisecond-based Redis operation, falling back to the legacy
	 * second-based operation if the precision API is not supported.
	 * <p>
	 * This method catches {@link UnsupportedOperationException} and {@link RedisConnectionFailureException}
	 * which typically indicate that the Redis server or driver does not support the precision API.
	 * In such cases, it logs a debug message and executes the legacy fallback operation.
	 *
	 * @param <T> the return type of the operation
	 * @param command the precision command type (e.g., PEXPIRE, PTTL)
	 * @param precisionSupplier supplier that executes the precision millisecond-based operation
	 * @param legacySupplier supplier that executes the legacy second-based operation as fallback
	 * @return the result of either the precision or legacy operation
	 * @throws RuntimeException if both precision and legacy operations fail
	 * @since 4.0.1
	 */
	public static <T> T withPrecisionFallback(PrecisionCommand command, Supplier<T> precisionSupplier,
			Supplier<T> legacySupplier) {

		try {
			return precisionSupplier.get();
		} catch (UnsupportedOperationException e) {
			if (logger.isTraceEnabled()) {
				logger.trace("Precision command '{}' not implemented by driver, using legacy fallback", command, e);
			}
			return legacySupplier.get();
		} catch (RedisConnectionFailureException e) {
			if (logger.isDebugEnabled()) {
				logger.debug(
						"Precision command '{}' not supported by Redis server (requires Redis 2.6+), "
								+ "falling back to legacy command. This may result in loss of sub-second precision.",
						command, e);
			}
			return legacySupplier.get();
		} catch (Exception e) {
			// Catch generic exceptions that might indicate unsupported operations
			if (logger.isDebugEnabled()) {
				logger.debug("Precision command '{}' failed, attempting legacy fallback", command, e);
			}
			return legacySupplier.get();
		}
	}

}
