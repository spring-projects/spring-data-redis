/*
 * Copyright 2015-2025 the original author or authors.
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

import org.mockito.internal.invocation.InvocationMatcher;
import org.mockito.internal.verification.api.VerificationData;
import org.mockito.invocation.Invocation;
import org.mockito.invocation.MatchableInvocation;
import org.mockito.verification.VerificationMode;
import org.springframework.util.StringUtils;

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
						return "%s for method: %s".formatted(mode, method);
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
