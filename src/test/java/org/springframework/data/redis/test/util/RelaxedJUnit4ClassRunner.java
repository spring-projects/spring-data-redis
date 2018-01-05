/*
 * Copyright 2014-2018 the original author or authors.
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

import java.lang.reflect.AnnotatedElement;

import org.junit.Ignore;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.springframework.data.redis.Version;
import org.springframework.data.redis.VersionParser;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSource;
import org.springframework.test.annotation.ProfileValueUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Extends the {@link SpringJUnit4ClassRunner} to accept {@code +} as wildcard for values tweaking comparison a litte so
 * that tests marked {@code IfProfileValue(name="varName" value="2.6+"} will be executed in
 * {@code 2.6, 2.6.1, 2.8, 3.0,... } environments.
 *
 * @author Christoph Strobl
 */
public class RelaxedJUnit4ClassRunner extends SpringJUnit4ClassRunner {

	public RelaxedJUnit4ClassRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
	}

	@Override
	protected boolean isTestMethodIgnored(FrameworkMethod frameworkMethod) {
		return isAnnotatedElementIgnored(frameworkMethod.getMethod());
	}

	private boolean isAnnotatedElementIgnored(AnnotatedElement annotatedElement) {

		if (annotatedElement.isAnnotationPresent(Ignore.class)) {
			return true;
		}

		IfProfileValue ifProfileValue = annotatedElement.getAnnotation(IfProfileValue.class);
		if (ifProfileValue == null) {
			return false;
		}

		String environmentValue = extractEnvironmentValue(ifProfileValue);
		return !isValidEnvironmentValue(environmentValue, ifProfileValue);
	}

	private boolean isValidEnvironmentValue(String environmentValue, IfProfileValue ifProfileValue) {

		for (String value : extractProfileValues(ifProfileValue)) {

			if (value.endsWith("+")) {

				Version expected = VersionParser.parseVersion(value.replace("+", ""));
				if (expected.compareTo(VersionParser.parseVersion(environmentValue)) <= 0) {
					return true;
				}

			} else {
				if (ObjectUtils.nullSafeEquals(value, environmentValue)) {
					return true;
				}
			}
		}
		return false;
	}

	private String extractEnvironmentValue(IfProfileValue ifProfileValue) {
		ProfileValueSource profileValueSource = ProfileValueUtils.retrieveProfileValueSource(getTestClass().getJavaClass());

		String environmentValue = profileValueSource.get(ifProfileValue.name());
		return environmentValue;
	}

	private String[] extractProfileValues(IfProfileValue ifProfileValue) {
		String[] annotatedValues = ifProfileValue.values();

		if (StringUtils.hasLength(ifProfileValue.value())) {
			if (annotatedValues.length > 0) {
				throw new IllegalArgumentException("Setting both the 'value' and 'values' attributes "
						+ "of @IfProfileValue is not allowed: choose one or the other.");
			}
			annotatedValues = new String[] { ifProfileValue.value() };
		}
		return annotatedValues;
	}
}
