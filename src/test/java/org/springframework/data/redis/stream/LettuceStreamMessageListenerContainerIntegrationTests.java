/*
 * Copyright 2018-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration tests for {@link StreamMessageListenerContainer} using Lettuce.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
@EnabledOnCommand("XREAD")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LettuceStreamMessageListenerContainerIntegrationTests
		extends AbstractStreamMessageListenerContainerIntegrationTests {

	private final RedisConnectionFactory connectionFactory;

	public LettuceStreamMessageListenerContainerIntegrationTests(RedisConnectionFactory connectionFactory) {
		super(connectionFactory);
		this.connectionFactory = connectionFactory;
	}

	@Test // GH-2568
	void shouldHaveAutoStartupEnabledByDefault() {

		StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder().build();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				StreamMessageListenerContainer.create(connectionFactory, options);

		assertThat(container.isAutoStartup()).isTrue();
	}

	@Test // GH-2568
	void shouldAllowConfiguringAutoStartup() {

		StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
				StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder().build();

		DefaultStreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
				new DefaultStreamMessageListenerContainer<>(connectionFactory, options);

		container.setAutoStartup(false);
		assertThat(container.isAutoStartup()).isFalse();

		container.setAutoStartup(true);
		assertThat(container.isAutoStartup()).isTrue();
	}
}
