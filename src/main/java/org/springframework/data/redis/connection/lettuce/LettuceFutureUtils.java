/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Utility methods to interact with {@link CompletableFuture} and {@link CompletionStage}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class LettuceFutureUtils {

	/**
	 * Creates a {@link CompletableFuture} that is completed {@link CompletableFuture#exceptionally(Function)
	 * exceptionally} given {@link Throwable}. This utility method allows exceptionally future creation with a single
	 * invocation.
	 *
	 * @param throwable must not be {@literal null}.
	 * @return the completed {@link CompletableFuture future}.
	 */
	static <T> CompletableFuture<T> failed(Throwable throwable) {

		Assert.notNull(throwable, "Throwable must not be null!");

		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(throwable);

		return future;
	}

	/**
	 * Synchronizes a {@link CompletableFuture} result by {@link CompletableFuture#join() waiting until the future is
	 * complete}. This method preserves {@link RuntimeException}s that may get thrown as result of future completion.
	 * Checked exceptions are thrown encapsulated within {@link java.util.concurrent.CompletionException}.
	 *
	 * @param future must not be {@literal null}.
	 * @throws RuntimeException thrown if the future is completed with a {@link RuntimeException}.
	 * @throws CompletionException thrown if the future is completed with a checked exception.
	 * @return the future result if completed normally.
	 */
	@Nullable
	static <T> T join(CompletionStage<T> future) throws RuntimeException, CompletionException {

		Assert.notNull(future, "CompletableFuture must not be null!");

		try {
			return future.toCompletableFuture().join();
		} catch (Exception e) {

			Throwable exceptionToUse = e;

			if (e instanceof CompletionException) {
				exceptionToUse = new LettuceExceptionConverter().convert((Exception) e.getCause());
				if (exceptionToUse == null) {
					exceptionToUse = e.getCause();
				}
			}

			if (exceptionToUse instanceof RuntimeException) {
				throw (RuntimeException) exceptionToUse;
			}

			throw new CompletionException(exceptionToUse);
		}
	}

	/**
	 * Returns a {@link Function} that ignores {@link CompletionStage#exceptionally(Function) exceptional completion} by
	 * recovering to {@code null}. This allows to progress with a previously failed {@link CompletionStage} without regard
	 * to the actual success/exception state.
	 *
	 * @return
	 */
	static <T> Function<Throwable, T> ignoreErrors() {
		return ignored -> null;
	}
}
