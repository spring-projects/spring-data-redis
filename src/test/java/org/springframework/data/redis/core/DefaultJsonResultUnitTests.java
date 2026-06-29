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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.core.DefaultJsonOperations.DefaultJsonResult;
import org.springframework.data.redis.serializer.JacksonRedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;

/**
 * Unit tests for {@link DefaultJsonResult}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
class DefaultJsonResultUnitTests {

	private final RedisJsonSerializer serializer = JacksonRedisJsonSerializer.createDefault();

	@Test
	void testAsClassDeserializesPojo() {

		Person person = new PersonObjectFactory().instance();
		DefaultJsonResult result = new DefaultJsonResult(serializer, serializer.serialize(person));

		assertThat(result.as(Person.class)).isEqualTo(person);
	}

	@Test
	void testAsTypeRefDeserializesGenericList() {

		DefaultJsonResult result = new DefaultJsonResult(serializer, "[1,2,3]");

		assertThat(result.as(new ParameterizedTypeReference<List<Long>>() {})).containsExactly(1L, 2L, 3L);
	}

	@Test
	void testAsStringReturnsRawPayload() {

		DefaultJsonResult result = new DefaultJsonResult(serializer, "{\"name\":\"rand\"}");

		assertThat(result.asString()).isEqualTo("{\"name\":\"rand\"}");
	}

	@Test
	void testAsBytesReturnsUtf8Encoding() {

		DefaultJsonResult result = new DefaultJsonResult(serializer, "{\"name\":\"rand\"}");

		assertThat(result.asBytes()).isEqualTo("{\"name\":\"rand\"}".getBytes(StandardCharsets.UTF_8));
	}

}
