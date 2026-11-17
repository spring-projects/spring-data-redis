/*
 * Copyright 2026-present the original author or authors.
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

import java.time.Duration;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

/**
 * Awaitility utility for consistent {@code await()} configuration.
 *
 * @author Mark Paluch
 */
public class ThingsToWaitFor {

	private static final Duration TIMEOUT = Duration.ofSeconds(3);

	/**
	 * Create a {@link ConditionFactory} without poll delay and using the default max timeout and a short {@code 10 ms}
	 * poll interval. Suitable for in-memory checks.
	 */
	public static ConditionFactory shortAwait() {
		return await().pollInterval(Duration.ofMillis(10));
	}

	/**
	 * Create a {@link ConditionFactory} without poll delay and using the default max timeout and a short {@code 10 ms}
	 * poll interval. Suitable for polling Redis commands.
	 */
	public static ConditionFactory await() {
		return Awaitility.await().pollDelay(Duration.ZERO).atMost(TIMEOUT);
	}

}
