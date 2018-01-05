/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.serializer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * Unit tests for {@link DefaultRedisElementWriter}.
 *
 * @author Mark Paluch
 */
public class DefaultRedisElementWriterUnitTests {

	@Test // DATAREDIS-602
	public void shouldSerializeInputCorrectly() {

		String input = "123ü?™";
		byte[] bytes = input.getBytes(StandardCharsets.UTF_8);

		DefaultRedisElementWriter<String> writer = new DefaultRedisElementWriter<>(
				new StringRedisSerializer(StandardCharsets.UTF_8));

		ByteBuffer result = writer.write(input);

		assertThat(result.array(), is(equalTo(bytes)));
	}

	@Test // DATAREDIS-602
	public void shouldWrapByteArrayForAbsentSerializer() {

		DefaultRedisElementWriter<Object> writer = new DefaultRedisElementWriter<>(null);

		byte[] input = { 1, 2, 3 };
		ByteBuffer result = writer.write(input);

		assertThat(result.array(), is(equalTo(input)));
	}

	@Test // DATAREDIS-602
	public void shouldPassThroughByteBufferForAbsentSerializer() {

		DefaultRedisElementWriter<Object> writer = new DefaultRedisElementWriter<>(null);

		byte[] input = { 1, 2, 3 };
		ByteBuffer result = writer.write(ByteBuffer.wrap(input));

		assertThat(result.array(), is(equalTo(input)));
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-602
	public void shouldFailForUnsupportedTypeWithAbsentSerializer() {

		DefaultRedisElementWriter<Object> writer = new DefaultRedisElementWriter<>(null);

		writer.write(new Object());
	}
}
