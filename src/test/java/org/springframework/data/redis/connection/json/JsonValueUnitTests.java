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
package org.springframework.data.redis.connection.json;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JsonValue}.
 *
 * @author Mark Paluch
 */
class JsonValueUnitTests {

	@Test // GH-3390
	void quotesSyntaxCharacters() {

		assertThat(JsonValue.of("").asString()).isEqualTo("\"\"");
		assertThat(JsonValue.of("plain").asString()).isEqualTo("\"plain\"");
		assertThat(JsonValue.of("\"\\").asString()).isEqualTo("\"\\\"\\\\\"");
		assertThat(JsonValue.of("\\\"").asString()).isEqualTo("\"\\\\\\\"\"");
	}

	@Test // GH-3390
	void quotesAllControlCharacters() {

		for (int i = 0; i < 0x20; i++) {
			assertThat(JsonValue.of(String.valueOf((char) i)).asString()).as("Control character U+%04X", i)
					.isEqualTo("\"\\u%04x\"".formatted(i));
		}
	}

	@Test // GH-3390
	void doesNotAllowJsonStringBreakout() {

		String value = "\"}],\"injected\":true,\"value\":\"";

		assertThat(JsonValue.of(value).asString()).isEqualTo("\"\\\"}],\\\"injected\\\":true,\\\"value\\\":\\\"\"");
	}

	@Test // GH-3390
	void preservesUnicodeCharacters() {

		String value = "Grüße\u2028\u2029𝄞😀";

		assertThat(JsonValue.of(value).asString()).isEqualTo('"' + value + '"');
	}

	@Test // GH-3390
	void acceptsOnlyFiniteValues() {

		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Double.NaN));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Double.POSITIVE_INFINITY));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Double.NEGATIVE_INFINITY));

		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Double) Double.NaN));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Double) Double.POSITIVE_INFINITY));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Double) Double.NEGATIVE_INFINITY));

		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Float.NaN));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Float.POSITIVE_INFINITY));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of(Float.NEGATIVE_INFINITY));

		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Float) Float.NaN));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Float) Float.POSITIVE_INFINITY));
		assertThatIllegalArgumentException().isThrownBy(() -> JsonValue.of((Float) Float.NEGATIVE_INFINITY));
	}

	@Test // GH-3390
	void shouldReturnNullValue() {

		assertThat(JsonValue.nullValue().asString()).isEqualTo("null");
		assertThat(JsonValue.nullValue().asBytes()).isEqualTo("null".getBytes());
	}

}
