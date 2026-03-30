/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.messaging;

import static org.mockito.Mockito.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.serializer.JdkSerializerMessageConverter;
import org.springframework.data.redis.serializer.RedisMessageConverters;
import org.springframework.messaging.MessageHeaders;

/**
 * Unit tests for {@link RedisMessageSendingTemplate}.
 *
 * @author Mark Paluch
 */
class RedisMessageSendingTemplateUnitTests {

	RedisOperations<String, String> operationsMock = mock(RedisOperations.class);
	RedisConnection connectionMock = mock(RedisConnection.class);
	RedisMessageSendingTemplate template;

	@BeforeEach
	void setUp() {
		when(operationsMock.execute(any(RedisCallback.class))).then(invocation -> {
			RedisCallback<?> callback = invocation.getArgument(0);
			return callback.doInRedis(connectionMock);
		});

		template = new RedisMessageSendingTemplate(operationsMock);
		template.setMessageConverter(RedisMessageConverters.createMessageConverter(
				it -> it.addCustomConverter(new JdkSerializerMessageConverter(getClass().getClassLoader()))));
		template.setDefaultDestination(ChannelTopic.of("default-channel"));
	}

	@Test
	void shouldSendStringWithTopicResolution() {

		template.convertAndSend("channel", "message");
		verify(connectionMock).publish(argThat(isBytes("channel")), argThat(isBytes("message")));
	}

	@Test
	void shouldSendStringToDefaultChannel() {

		template.convertAndSend("message");
		verify(connectionMock).publish(argThat(isBytes("default-channel")), argThat(isBytes("message")));
	}

	@Test
	void shouldSendStringMessage() {

		template.convertAndSend(ChannelTopic.of("channel"), "message");
		verify(connectionMock).publish(argThat(isBytes("channel")), argThat(isBytes("message")));
	}

	@Test
	void shouldSendJsonMessage() {

		template.convertAndSend(ChannelTopic.of("channel"), new Person("White", "Walter"));
		verify(connectionMock).publish(argThat(isBytes("channel")),
				argThat(isBytes("{\"lastName\":\"White\",\"firstName\":\"Walter\"}")));
	}

	@Test
	void shouldSendStringAsJson() {

		template.convertAndSend(ChannelTopic.of("channel"), "message",
				Map.of(MessageHeaders.CONTENT_TYPE, "application/json"));
		verify(connectionMock).publish(argThat(isBytes("channel")), argThat(isBytes("\"message\"")));
	}

	@Test
	void shouldSendStringAsJdkSerialized() {

		template.convertAndSend(ChannelTopic.of("channel"), new SerializablePerson("foo", "bar"),
				Map.of(MessageHeaders.CONTENT_TYPE, JdkSerializerMessageConverter.APPLICATION_JAVA_SERIALIZED_OBJECT_VALUE));
		verify(connectionMock).publish(argThat(isBytes("channel")), any(byte[].class));
	}

	record Person(String lastName, String firstName) {
	}

	record SerializablePerson(String lastName, String firstName) implements Serializable {
	}

	static ArgumentMatcher<byte[]> isBytes(String value) {
		return new ArgumentMatcher<>() {
			@Override
			public boolean matches(byte[] argument) {
				return Arrays.equals(argument, value.getBytes());
			}

			@Override
			public String toString() {
				return "\"%s\".getBytes()".formatted(value);
			}
		};
	}

}
