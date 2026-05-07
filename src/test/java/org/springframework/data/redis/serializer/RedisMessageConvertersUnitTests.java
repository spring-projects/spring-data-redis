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
package org.springframework.data.redis.serializer;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Unit tests for {@link RedisMessageConverters}.
 *
 * @author Mark Paluch
 */
class RedisMessageConvertersUnitTests {

	@Test // GH-3357
	void serializesObjectToJson() {

		MessageConverter messageConverter = RedisMessageConverters.createMessageConverter();

		Person person = new Person();
		person.setName("Walter");
		MessageHeaders messageHeaders = new MessageHeaders(Map.of(MessageHeaders.CONTENT_TYPE, "application/json"));
		Message<?> message = messageConverter.toMessage(person, messageHeaders);

		String json = new String((byte[]) message.getPayload());
		assertThat(json).contains("name", "Walter").doesNotContain("class");
	}

	@Test // GH-3357
	void deserializesJsonToObject() {

		MessageConverter messageConverter = RedisMessageConverters.createMessageConverter();

		String json = "{\"@class\":\"org.springframework.data.redis.serializer.RedisMessageConvertersUnitTests$Hey\",\"name\":\"Walter\"}";
		MessageHeaders messageHeaders = new MessageHeaders(Map.of(MessageHeaders.CONTENT_TYPE, "application/json"));
		Message<byte[]> message = MessageBuilder.createMessage(json.getBytes(), messageHeaders);

		assertThat(messageConverter.fromMessage(message, Object.class)).isInstanceOf(Map.class);
		assertThat(messageConverter.fromMessage(message, Person.class)).isInstanceOf(Person.class);
	}

	static class Person {

		String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	static class Hey {

		String name;

		public Hey() {
			throw new RuntimeException("nope");
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

}
