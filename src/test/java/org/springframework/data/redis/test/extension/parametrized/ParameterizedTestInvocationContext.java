/*
 * Copyright 2020-2023 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

/**
 * Copy of {@code org.junit.jupiter.params.ParameterizedTestInvocationContext}.
 */
class ParameterizedTestInvocationContext implements TestTemplateInvocationContext {

	private final ParameterizedTestNameFormatter formatter;
	private final ParameterizedTestContext constructorContext;
	private final ParameterizedTestContext methodContext;
	private final Object[] arguments;

	ParameterizedTestInvocationContext(ParameterizedTestNameFormatter formatter,
			ParameterizedTestContext constructorContext, ParameterizedTestContext methodContext, Object[] arguments) {
		this.formatter = formatter;
		this.constructorContext = constructorContext;
		this.methodContext = methodContext;
		this.arguments = arguments;
	}

	@Override
	public String getDisplayName(int invocationIndex) {
		return this.formatter.format(invocationIndex, this.arguments);
	}

	@Override
	public List<Extension> getAdditionalExtensions() {
		return Collections.singletonList(
				new ParameterizedTestParameterResolver(this.constructorContext, this.methodContext, this.arguments));
	}

}
