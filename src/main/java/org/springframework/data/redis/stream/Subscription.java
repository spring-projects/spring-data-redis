/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.stream;

import java.time.Duration;

/**
 * The {@link Subscription} is the link to the actual running {@link Task}.
 * <p />
 * Due to the asynchronous nature of the {@link Task} execution a {@link Subscription} might not immediately become
 * active. {@link #isActive()} provides an answer if the underlying {@link Task} is already running.
 * <p />
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface Subscription extends Cancelable {

	/**
	 * @return {@literal true} if the subscription is currently executed.
	 */
	boolean isActive();

	/**
	 * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} becomes {@link #isActive()
	 * active} or {@link Duration timeout} exceeds.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return {@code true} if the subscription was activated. {@code false} if the waiting time elapsed before task was
	 *         activated.
	 * @throws InterruptedException if the current thread is interrupted while waiting.
	 */
	boolean await(Duration timeout) throws InterruptedException;
}
