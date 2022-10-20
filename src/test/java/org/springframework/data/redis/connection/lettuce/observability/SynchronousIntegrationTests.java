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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;

/**
 * Collection of tests that log metrics and tracing using the synchronous API.
 *
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestConfig.class)
public class SynchronousIntegrationTests extends SampleTestRunner {

	@Autowired LettuceConnectionFactory connectionFactory;

	SynchronousIntegrationTests() {
		super(SampleRunnerConfig.builder().build());
	}

	@Override
	protected MeterRegistry createMeterRegistry() {
		return TestConfig.METER_REGISTRY;
	}

	@Override
	protected ObservationRegistry createObservationRegistry() {
		return TestConfig.OBSERVATION_REGISTRY;
	}

	@Override
	public SampleTestRunnerConsumer yourCode() {

		return (tracer, meterRegistry) -> {

			RedisConnection connection = connectionFactory.getConnection();
			connection.ping();

			connection.close();

			assertThat(tracer.getFinishedSpans()).isNotEmpty();
			System.out.println(((SimpleMeterRegistry) meterRegistry).getMetersAsString());

			assertThat(tracer.getFinishedSpans()).isNotEmpty();

			for (FinishedSpan finishedSpan : tracer.getFinishedSpans()) {
				assertThat(finishedSpan.getTags()).containsEntry("db.system", "redis")
						.containsEntry("net.sock.peer.addr", SettingsUtils.getHost())
						.containsEntry("net.sock.peer.port", "" + SettingsUtils.getPort());
				assertThat(finishedSpan.getTags()).containsKeys("db.operation");
			}
		};
	}

}
