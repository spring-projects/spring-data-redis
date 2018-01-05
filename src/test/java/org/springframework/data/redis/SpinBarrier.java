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
package org.springframework.data.redis;

/**
 * @author Jennifer Hickey
 */
abstract public class SpinBarrier {

	/**
	 * Periodically tests for a condition until it is met or a timeout occurs
	 *
	 * @param condition The condition to periodically test
	 * @param timeout The timeout
	 * @return true if condition passes, false if condition does not pass within timeout
	 */
	public static boolean waitFor(TestCondition condition, long timeout) {
		boolean passes = false;
		for (long currentTime = System.currentTimeMillis(); System.currentTimeMillis() - currentTime < timeout;) {
			if (condition.passes()) {
				passes = true;
				break;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
		return passes;
	}
}
