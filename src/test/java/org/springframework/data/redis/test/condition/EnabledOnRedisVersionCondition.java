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

import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.springframework.data.redis.test.extension.LettuceExtension;

/**
 * {@link ExecutionCondition} for {@link EnabledOnRedisVersionCondition @EnabledOnVersion}.
 *
 * @author Mark Paluch return ENABLED_BY_DEFAULT;
 * @see EnabledOnRedisVersionCondition
 */
class EnabledOnRedisVersionCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled("@EnabledOnVersion is not present");
	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
			.create(EnabledOnRedisVersionCondition.class);

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnRedisVersion> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnRedisVersion.class);

		if (!optional.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		String requiredVersion = optional.get().value();

		ExtensionContext.Store store = context.getStore(NAMESPACE);
		RedisConditions conditions = store.getOrComputeIfAbsent(RedisConditions.class, ignore -> {

			StatefulRedisConnection connection = new LettuceExtension().resolve(context, StatefulRedisConnection.class);
			return RedisConditions.of(connection);
		}, RedisConditions.class);

		boolean versionMet = conditions.hasVersionGreaterOrEqualsTo(requiredVersion);
		return versionMet
				? enabled(
						String.format("Enabled on version %s (actual version: %s)", requiredVersion, conditions.getRedisVersion()))
				: disabled(String.format("Disabled, version %s not available on Redis version %s", requiredVersion,
						conditions.getRedisVersion()));
	}
}
