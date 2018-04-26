/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Helper class featuring methods for calculating Redis timeouts
 *
 * @author Jennifer Hickey
 * @author Mark Paluch
 * @author Christoph Strobl
 */
abstract public class TimeoutUtils {

	/**
	 * Check if a given Duration can be represented in {@code sec} or requires {@code msec} representation.
	 *
	 * @param duration the actual {@link Duration} to inspect. Never {@literal null}.
	 * @return {@literal true} if the {@link Duration} contains millisecond information.
	 * @since 2.1
	 */
	public static boolean hasMillis(Duration duration) {
		return duration.toMillis() % 1000 != 0;
	}

	/**
	 * Converts the given timeout to seconds.
	 * <p>
	 * Since a 0 timeout blocks some Redis ops indefinitely, this method will return 1 if the original value is greater
	 * than 0 but is truncated to 0 on conversion.
	 *
	 * @param timeout The timeout to convert
	 * @param unit The timeout's unit
	 * @return The converted timeout
	 */
	public static long toSeconds(long timeout, TimeUnit unit) {
		return roundUpIfNecessary(timeout, unit.toSeconds(timeout));

	}

	/**
	 * Converts the given timeout to milliseconds.
	 * <p>
	 * Since a 0 timeout blocks some Redis ops indefinitely, this method will return 1 if the original value is greater
	 * than 0 but is truncated to 0 on conversion.
	 *
	 * @param timeout The timeout to convert
	 * @param unit The timeout's unit
	 * @return The converted timeout
	 */
	public static long toMillis(long timeout, TimeUnit unit) {
		return roundUpIfNecessary(timeout, unit.toMillis(timeout));
	}

	private static long roundUpIfNecessary(long timeout, long convertedTimeout) {
		// A 0 timeout blocks some Redis ops indefinitely, round up if that's
		// not the intention
		if (timeout > 0 && convertedTimeout == 0) {
			return 1;
		}
		return convertedTimeout;
	}
}
