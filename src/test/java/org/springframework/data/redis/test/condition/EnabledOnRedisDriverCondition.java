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
package org.springframework.data.redis.test.condition;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.function.Try;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * {@link ExecutionCondition} for {@link EnabledOnRedisDriverCondition @EnabledOnRedisDriver}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see EnabledOnRedisDriver
 */
class EnabledOnRedisDriverCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled("@EnabledOnRedisDriver is not present");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnRedisDriver> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnRedisDriver.class);

		if (!optional.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		EnabledOnRedisDriver annotation = optional.get();
		Class<?> testClass = context.getRequiredTestClass();

		List<Field> annotatedFields = AnnotationUtils.findAnnotatedFields(testClass,
				EnabledOnRedisDriver.DriverQualifier.class, it -> RedisConnectionFactory.class.isAssignableFrom(it.getType()));

		if (annotatedFields.isEmpty()) {
			throw new IllegalStateException(
					"@EnabledOnRedisDriver requires a field of type RedisConnectionFactory annotated with @DriverQualifier!");
		}

		for (Field field : annotatedFields) {
			Try<Object> fieldValue = ReflectionUtils.tryToReadFieldValue(field, context.getRequiredTestInstance());

			RedisConnectionFactory value = (RedisConnectionFactory) fieldValue
					.getOrThrow(e -> new IllegalStateException("Cannot read field " + field, e));

			boolean foundMatch = false;
			for (RedisDriver redisDriver : annotation.value()) {
				if (redisDriver.matches(value)) {
					foundMatch = true;
				}
			}

			if (!foundMatch) {
				return disabled(String.format("Driver %s not supported. Supported driver(s): %s", value,
						Arrays.toString(annotation.value())));
			}
		}

		return enabled("Found enabled driver(s): " + Arrays.toString(annotation.value()));

	}

}
