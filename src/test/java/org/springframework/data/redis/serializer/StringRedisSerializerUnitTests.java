/*
 * Copyright 2017-2019 the original author or authors.
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

import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * Unit tests for {@link StringRedisSerializer}.
 *
 * @author Mark Paluch
 */
public class StringRedisSerializerUnitTests {

	@Test
	public void shouldSerializeToAscii() {

		assertThat(StringRedisSerializer.US_ASCII.serialize("foo-bar")).isEqualTo("foo-bar".getBytes());
		assertThat(StringRedisSerializer.US_ASCII.serialize("üßØ")).isEqualTo("???".getBytes());
	}

	@Test
	public void shouldDeserializeFromAscii() {

		assertThat(StringRedisSerializer.US_ASCII.deserialize("foo-bar".getBytes())).isEqualTo("foo-bar");
	}

	@Test
	public void shouldSerializeToIso88591() {

		assertThat(StringRedisSerializer.ISO_8859_1.serialize("üßØ"))
				.isEqualTo("üßØ".getBytes(StandardCharsets.ISO_8859_1));
	}

	@Test
	public void shouldDeserializeFromIso88591() {

		assertThat(StringRedisSerializer.ISO_8859_1.deserialize("üßØ".getBytes(StandardCharsets.ISO_8859_1)))
				.isEqualTo("üßØ");
	}

	@Test
	public void shouldSerializeToUtf8() {

		assertThat(StringRedisSerializer.UTF_8.serialize("foo-bar")).isEqualTo("foo-bar".getBytes());
		assertThat(StringRedisSerializer.UTF_8.serialize("üßØ")).isEqualTo("üßØ".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void shouldDeserializeFromUtf8() {
		assertThat(StringRedisSerializer.UTF_8.deserialize("üßØ".getBytes(StandardCharsets.UTF_8))).isEqualTo("üßØ");
	}
}
