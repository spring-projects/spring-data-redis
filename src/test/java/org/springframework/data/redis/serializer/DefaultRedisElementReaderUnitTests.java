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
 * Unit tests for {@link DefaultRedisElementReader}.
 *
 * @author Mark Paluch
 */
public class DefaultRedisElementReaderUnitTests {

	@Test // DATAREDIS-602
	public void shouldDecodeByteBufferCorrectly() {

		String input = "123ü?™";
		byte[] bytes = input.getBytes(StandardCharsets.UTF_8);

		DefaultRedisElementReader<String> reader = new DefaultRedisElementReader<>(
				new StringRedisSerializer(StandardCharsets.UTF_8));

		String result = reader.read(ByteBuffer.wrap(bytes));

		assertThat(result, is(equalTo(input)));
	}

	@Test // DATAREDIS-602
	public void shouldPassThroughByteBufferForAbsentSerializer() {

		ByteBuffer input = ByteBuffer.allocate(1);

		DefaultRedisElementReader<Object> reader = new DefaultRedisElementReader<>(null);

		Object result = reader.read(input);

		assertThat(result, is(equalTo(input)));
	}
}
