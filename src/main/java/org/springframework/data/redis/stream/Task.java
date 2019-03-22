/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.stream;

import java.time.Duration;

import org.springframework.scheduling.SchedulingAwareRunnable;

/**
 * The actual {@link Task} to run within the {@link StreamMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface Task extends SchedulingAwareRunnable, Cancelable {

	/**
	 * @return {@literal true} if the task is currently {@link State#RUNNING running}.
	 */
	default boolean isActive() {
		return State.RUNNING.equals(getState());
	}

	/**
	 * Get the current lifecycle phase.
	 *
	 * @return never {@literal null}.
	 */
	State getState();

	/**
	 * Synchronous, <strong>blocking</strong> call that awaits until this {@link Task} becomes active. Start awaiting is
	 * rearmed after {@link #cancel() cancelling} to support restart.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return {@code true} if the task was started. {@code false} if the waiting time elapsed before task was started.
	 * @throws InterruptedException if the current thread is interrupted while waiting.
	 */
	boolean awaitStart(Duration timeout) throws InterruptedException;

	/**
	 * The {@link Task.State} defining the lifecycle phase the actual {@link Task}.
	 *
	 * @author Mark Paluch
	 * @since 2.2
	 */
	enum State {
		CREATED, STARTING, RUNNING, CANCELLED;
	}
}
