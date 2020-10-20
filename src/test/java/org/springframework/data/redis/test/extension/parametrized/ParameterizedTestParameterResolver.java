/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.test.extension.parametrized;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Copy of {@code org.junit.jupiter.params.ParameterizedTestParameterResolver}.
 */
class ParameterizedTestParameterResolver implements ParameterResolver {

	private final ParameterizedTestContext constructorContext;
	private final ParameterizedTestContext methodContext;
	private final Object[] arguments;

	ParameterizedTestParameterResolver(ParameterizedTestContext constructorContext,
			ParameterizedTestContext methodContext, Object[] arguments) {
		this.constructorContext = constructorContext;
		this.methodContext = methodContext;
		this.arguments = arguments;
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {

		Executable declaringExecutable = parameterContext.getDeclaringExecutable();
		int parameterIndex = parameterContext.getIndex();
		ParameterizedTestContext testContext;

		if (declaringExecutable instanceof Constructor) {
			testContext = this.constructorContext;
		} else {

			Method testMethod = extensionContext.getTestMethod().orElse(null);

			// Not a parameterized method?
			if (parameterContext.getDeclaringExecutable() instanceof Method && !declaringExecutable.equals(testMethod)) {
				return false;
			}
			testContext = this.methodContext;
		}

		// Current parameter is an aggregator?
		if (testContext.isAggregator(parameterIndex)) {
			return true;
		}

		// Ensure that the current parameter is declared before aggregators.
		// Otherwise, a different ParameterResolver should handle it.
		if (testContext.hasAggregator()) {
			return parameterIndex < testContext.indexOfFirstAggregator();
		}

		// Else fallback to behavior for parameterized test methods without aggregators.
		return parameterIndex < this.arguments.length;
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		if (parameterContext.getDeclaringExecutable() instanceof Constructor) {
			return this.constructorContext.resolve(parameterContext, this.arguments);
		}

		return this.methodContext.resolve(parameterContext, this.arguments);
	}

}
