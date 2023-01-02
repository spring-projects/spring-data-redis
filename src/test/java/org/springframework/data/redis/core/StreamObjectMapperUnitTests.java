/*
 * Copyright 2022-2023 the original author or authors.
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

import lombok.Data;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;

/**
 * Unit tests for {@link StreamObjectMapper}.
 *
 * @author Mark Paluch
 */
class StreamObjectMapperUnitTests {

	@Test // GH-2198
	void shouldRetainTypeHintUsingObjectHashMapper() {

		StreamObjectMapper mapper = new StreamObjectMapper(ObjectHashMapper.getSharedInstance());

		MyType result = mapper.getHashMapper(MyType.class)
				.fromHash(Collections.singletonMap("value".getBytes(), "hello".getBytes()));

		assertThat(result.value).isEqualTo("hello");
	}

	@Test // GH-2198
	void shouldRetainTypeHintUsingJackson() {

		StreamObjectMapper mapper = new StreamObjectMapper(new Jackson2HashMapper(true));

		MyType result = mapper.getHashMapper(MyType.class).fromHash(Collections.singletonMap("value", "hello"));

		assertThat(result.value).isEqualTo("hello");
	}

	@Data
	static class MyType {
		String value;
	}

}
