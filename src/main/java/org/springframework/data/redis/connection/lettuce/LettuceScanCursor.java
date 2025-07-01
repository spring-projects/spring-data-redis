/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import java.util.Collection;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Lettuce-specific {@link ScanCursor} extension that maintains the cursor state that is required for stateful-scanning
 * across a Redis Cluster.
 * <p>
 * The cursor state uses Lettuce's stateful {@link io.lettuce.core.ScanCursor} to keep track of scanning progress.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
abstract class LettuceScanCursor<T> extends ScanCursor<T> {

	private io.lettuce.core.@Nullable ScanCursor state;

	/**
	 * Creates a new {@link LettuceScanCursor} given {@link ScanOptions}.
	 *
	 * @param options must not be {@literal null}.
	 */
	LettuceScanCursor(ScanOptions options) {
		super(options);
	}

	@Override
	protected ScanIteration<T> doScan(CursorId cursorId, ScanOptions options) {

		if (state == null && cursorId.isInitial()) {
			return scanAndProcessState(io.lettuce.core.ScanCursor.INITIAL, options);
		}

		if (state != null) {

			if (isMatchingCursor(cursorId)) {
				return scanAndProcessState(state, options);
			}
		}

		throw new IllegalArgumentException("Current scan %s state and cursor %s do not match"
				.formatted(state != null ? state.getCursor() : "(none)", cursorId));
	}

	@Override
	protected boolean isFinished(CursorId cursorId) {
		return state != null && isMatchingCursor(cursorId) ? state.isFinished() : super.isFinished(cursorId);
	}

	private ScanIteration<T> scanAndProcessState(io.lettuce.core.ScanCursor scanCursor, ScanOptions options) {

		LettuceScanIteration<T> iteration = doScan(scanCursor, options);
		state = iteration.cursor;

		return iteration;
	}

	private boolean isMatchingCursor(CursorId cursorId) {
		return state != null && state.getCursor().equals(cursorId.getCursorId());
	}

	/**
	 * Perform the actual scan operation given {@link io.lettuce.core.ScanCursor} and {@link ScanOptions}.
	 *
	 * @param cursor must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}
	 */
	protected abstract LettuceScanIteration<T> doScan(io.lettuce.core.ScanCursor cursor, ScanOptions options);

	/**
	 * Lettuce-specific extension to {@link ScanIteration} keeping track of the original
	 * {@link io.lettuce.core.ScanCursor} object.
	 *
	 * @author Mark Paluch
	 */
	static class LettuceScanIteration<T> extends ScanIteration<T> {

		private final io.lettuce.core.ScanCursor cursor;

		LettuceScanIteration(io.lettuce.core.ScanCursor cursor, Collection<T> items) {

			super(CursorId.of(cursor.getCursor()), items);
			this.cursor = cursor;
		}
	}
}
