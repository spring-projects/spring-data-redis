/*
 * Copyright 2020-2025 the original author or authors.
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

import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.springframework.data.redis.SettingsUtils;

/**
 * {@link ExecutionCondition} for {@link EnabledOnRedisSentinelCondition @EnabledOnRedisSentinelAvailable}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see EnabledOnRedisSentinelCondition
 */
class EnabledOnRedisSentinelCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled(
			"@EnabledOnSentinelAvailable is not present");

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnRedisSentinelAvailable> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnRedisSentinelAvailable.class);

		if (!optional.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		EnabledOnRedisSentinelAvailable annotation = optional.get();

		if (RedisDetector.canConnectToPort(annotation.value())) {

			return enabled("Connection successful to Redis Sentinel at %s:%d".formatted(SettingsUtils.getHost(),
					annotation.value()));
		}

		return disabled("Cannot connect to Redis Sentinel at %s:%d".formatted(SettingsUtils.getHost(),
				annotation.value()));
	}
}
