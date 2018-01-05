/*
 * Copyright 2015-2018 the original author or authors.
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
 * @author Christoph Strobl
 * @since 1.7
 */
public class MockitoUtils {

	/**
	 * Verifies a given method is called a total number of times across all given mocks.
	 *
	 * @param method
	 * @param mode
	 * @param mocks
	 */
	@SuppressWarnings({ "rawtypes", "serial" })
	public static void verifyInvocationsAcross(final String method, final VerificationMode mode, Object... mocks) {

		mode.verify(new VerificationDataImpl(getInvocations(method, mocks), new InvocationMatcher(null, Collections
				.singletonList(org.mockito.internal.matchers.Any.ANY)) {

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

		@Override
		public InvocationMatcher getWanted() {
			return wanted;
		}

	}

}
