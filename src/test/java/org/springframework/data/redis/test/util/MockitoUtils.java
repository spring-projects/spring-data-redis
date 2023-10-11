/*
 * Copyright 2015-2023 the original author or authors.
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
package org.springframework.data.redis.test.util;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.mockito.internal.invocation.InvocationMatcher;
import org.mockito.internal.verification.api.VerificationData;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.MatchableInvocation;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.util.function.ThrowingFunction;

/**
 * Utilities for using {@literal Mockito} and creating {@link Object mock objects} in {@literal unit tests}.
 *
 * @author Christoph Strobl
 * @author John Blum
 * @see org.mockito.Mockito
 * @since 1.7
 */
@SuppressWarnings("unused")
public abstract class MockitoUtils {

	/**
	 * Creates a mock {@link Future} returning the given {@link Object result}.
	 *
	 * @param <T> {@link Class type} of {@link Object result} returned by the mock {@link Future}.
	 * @param result {@link Object value} returned as the {@literal result} of the mock {@link Future}.
	 * @return a new mock {@link Future}.
	 * @see java.util.concurrent.Future
	 */
	@SuppressWarnings("unchecked")
	public static <T> Future<T> mockFuture(@Nullable T result) {

		try {

			AtomicBoolean cancelled = new AtomicBoolean(false);
			AtomicBoolean done = new AtomicBoolean(false);

			Future<T> mockFuture = mock(Future.class, withSettings().strictness(Strictness.LENIENT));

			// A Future can only be cancelled if not done, it was not already cancelled, and no error occurred.
			// The cancel(..) logic is not Thread-safe due to compound actions involving multiple variables.
			// However, the cancel(..) logic does not necessarily need to be Thread-safe given the task execution
			// of a Future is asynchronous and cancellation is driven by Thread interrupt from another Thread.
			Answer<Boolean> cancelAnswer = invocation -> !done.get() && cancelled.compareAndSet(done.get(), true)
					&& done.compareAndSet(done.get(), true);

			Answer<T> getAnswer = invocation -> {

				// The Future is done no matter if it returns the result or was cancelled/interrupted.
				done.set(true);

				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException("Thread was interrupted");
				}

				if (cancelled.get()) {
					throw new CancellationException("Task was cancelled");
				}

				return result;
			};

			doAnswer(invocation -> cancelled.get()).when(mockFuture).isCancelled();
			doAnswer(invocation -> done.get()).when(mockFuture).isDone();
			doAnswer(cancelAnswer).when(mockFuture).cancel(anyBoolean());
			doAnswer(getAnswer).when(mockFuture).get();
			doAnswer(getAnswer).when(mockFuture).get(anyLong(), isA(TimeUnit.class));

			return mockFuture;
		} catch (Exception cause) {
			String message = String.format("Failed to create a mock of Future having result [%s]", result);
			throw new IllegalStateException(message, cause);
		}
	}

	/**
	 * Creates a mock {@link Future} returning the given {@link Object result}, customized with the given, required
	 * {@link Function}.
	 *
	 * @param <T> {@link Class type} of {@link Object result} returned by the mock {@link Future}.
	 * @param result {@link Object value} returned as the {@literal result} of the mock {@link Future}.
	 * @param futureFunction {@link Function} used to customize the mock {@link Future} on creation; must not be
	 *          {@literal null}.
	 * @return a new mock {@link Future}.
	 * @see #mockFuture(Object)
	 */
	public static <T> Future<T> mockFuture(@Nullable T result, ThrowingFunction<Future<T>, Future<T>> futureFunction) {

		Future<T> mockFuture = mockFuture(result);

		return futureFunction.apply(mockFuture);
	}

	/**
	 * Verifies a given method is called once across all given mocks.
	 *
	 * @param method {@link String name} of a {@link java.lang.reflect.Method} on the {@link Object mock object}.
	 * @param mocks array of {@link Object mock objects} to verify.
	 */
	public static void verifyInvocationsAcross(String method, Object... mocks) {
		verifyInvocationsAcross(method, times(1), mocks);
	}

	/**
	 * Verifies a given method is called a total number of times across all given mocks.
	 *
	 * @param method {@link String name} of a {@link java.lang.reflect.Method} on the {@link Object mock object}.
	 * @param mode mode of verification used by {@literal Mockito} to verify invocations on {@link Object mock objects}.
	 * @param mocks array of {@link Object mock objects} to verify.
	 */
	public static void verifyInvocationsAcross(String method, VerificationMode mode, Object... mocks) {

		mode.verify(new VerificationDataImpl(getInvocations(method, mocks),
				new InvocationMatcher(null, Collections.singletonList(org.mockito.internal.matchers.Any.ANY)) {

					@Override
					public boolean matches(Invocation actual) {
						return true;
					}

					@Override
					public String toString() {
						return String.format("%s for method: %s", mode, method);
					}
				}));
	}

	private static List<Invocation> getInvocations(String method, Object... mocks) {

		List<Invocation> invocations = new ArrayList<>();

		for (Object mock : mocks) {
			if (StringUtils.hasText(method)) {
				for (Invocation invocation : mockingDetails(mock).getInvocations()) {
					if (invocation.getMethod().getName().equals(method)) {
						invocations.add(invocation);
					}
				}
			} else {
				invocations.addAll(mockingDetails(mock).getInvocations());
			}
		}

		return invocations;
	}

	static class VerificationDataImpl implements VerificationData {

		private final List<Invocation> invocations;
		private final InvocationMatcher wanted;

		public VerificationDataImpl(List<Invocation> invocations, InvocationMatcher wanted) {
			this.invocations = invocations;
			this.wanted = wanted;
		}

		@Override
		public List<Invocation> getAllInvocations() {
			return invocations;
		}

		@Override
		public MatchableInvocation getTarget() {
			return wanted;
		}
	}

}
