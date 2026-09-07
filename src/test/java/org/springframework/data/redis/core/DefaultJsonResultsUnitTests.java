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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonResult;
import org.springframework.data.redis.core.RedisJsonTemplate.DefaultJsonResults;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;

/**
 * Unit tests for {@link DefaultJsonResults}.
 *
 * @author Yordan Tsintsov
 * @author Moritz Halbritter
 * @since 4.2
 */
class DefaultJsonResultsUnitTests {

	private final RedisJsonSerializer serializer = GenericJacksonJsonRedisSerializer.builder().build();

	@Test
	void testAsClassDeserializesEachEntry() {

		List<JsonOperations.JsonResult> data = List.of(
				DefaultJsonResult.ofValue(serializer, "1".getBytes()), DefaultJsonResult.ofValue(serializer, "2".getBytes()),
				DefaultJsonResult.ofValue(serializer, "3".getBytes()));

		DefaultJsonResults result = new DefaultJsonResults(data);

		assertThat(result.as(Long.class)).containsExactly(1L, 2L, 3L);
	}

	@Test
	void testAsStringReturnsRawEntries() {

		List<JsonOperations.JsonResult> data = List.of(
				DefaultJsonResult.ofValue(serializer, "{\"a\":1}".getBytes()),
				DefaultJsonResult.ofMatchArray(serializer, null),
				DefaultJsonResult.ofValue(serializer, "{\"a\":2}".getBytes()));

		assertThat(new DefaultJsonResults(data).asString()).isEqualTo(Arrays.asList("{\"a\":1}", null, "{\"a\":2}"));
	}

	@Test
	void testAsBytesReturnsUtf8EncodingPerEntry() {

		List<JsonOperations.JsonResult> data = List.of(
				DefaultJsonResult.ofValue(serializer, "foo".getBytes()),
				DefaultJsonResult.ofMatchArray(serializer, null));

		DefaultJsonResults result = new DefaultJsonResults(data);

		List<byte[]> bytes = result.asBytes();

		assertThat(bytes).hasSize(2);
		assertThat(bytes.get(0)).isEqualTo("foo".getBytes());
		assertThat(bytes.get(1)).isNull();
	}

	@Test
	void testIsEmptyReturnValue() {

		DefaultJsonResults emptyResult = new DefaultJsonResults(new ArrayList<>());
		DefaultJsonResults correctResult = new DefaultJsonResults(
				List.of(DefaultJsonResult.ofValue(serializer, "1".getBytes())));

		assertThat(emptyResult.isEmpty()).isTrue();
		assertThat(correctResult.isEmpty()).isFalse();
	}

}
