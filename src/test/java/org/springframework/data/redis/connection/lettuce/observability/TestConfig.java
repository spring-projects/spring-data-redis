/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce.observability;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;

/**
 * @author Mark Paluch
 */
@Configuration
class TestConfig {

	static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
	static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

	static {
		OBSERVATION_REGISTRY.observationConfig().observationHandler(new DefaultMeterObservationHandler(METER_REGISTRY));
	}

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources(ObservationRegistry observationRegistry) {
		return ClientResources.builder().tracing(new MicrometerTracingAdapter(observationRegistry, "Redis", true))
				.build();
	}

	@Bean
	LettuceConnectionFactory connectionFactory(ClientResources clientResources) {

		LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
				.clientResources(clientResources).build();

		return new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration(), clientConfiguration);
	}

	@Bean
	ObservationRegistry registry() {
		return OBSERVATION_REGISTRY;
	}
}
