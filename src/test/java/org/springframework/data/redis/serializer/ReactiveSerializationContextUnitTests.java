/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Unit tests for {@link ReactiveSerializationContext}.
 *
 * @author Mark Paluch
 */
public class ReactiveSerializationContextUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfKeySerializerIsNotSet() {

		ReactiveSerializationContext.<String, String> builder() //
				.value(new StringRedisSerializer()) //
				.hashKey(new StringRedisSerializer()) //
				.hashValue(new StringRedisSerializer()) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfValueSerializerIsNotSet() {

		ReactiveSerializationContext.<String, String> builder() //
				.key(new StringRedisSerializer()) //
				.hashKey(new StringRedisSerializer()) //
				.hashValue(new StringRedisSerializer()) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfHashKeySerializerIsNotSet() {

		ReactiveSerializationContext.<String, String> builder() //
				.key(new StringRedisSerializer()) //
				.value(new StringRedisSerializer()) //
				.hashValue(new StringRedisSerializer()) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfHashValueSerializerIsNotSet() {

		ReactiveSerializationContext.<String, String> builder() //
				.key(new StringRedisSerializer()) //
				.value(new StringRedisSerializer()) //
				.hashKey(new StringRedisSerializer()) //
				.build();
	}

	@Test // DATAREDIS-602
	public void shouldBuildSerializationContext() {

		ReactiveSerializationContext<String, Long> serializationContext = createSerializationContext();

		assertThat(serializationContext.key()).isNotNull();
		assertThat(serializationContext.value()).isNotNull();
		assertThat(serializationContext.hashKey()).isNotNull();
		assertThat(serializationContext.hashValue()).isNotNull();
		assertThat(serializationContext.string()).isNotNull();
	}

	@Test // DATAREDIS-602
	public void shouldEncodeAndDecodeKey() {

		ReactiveSerializationContext<String, Long> serializationContext = createSerializationContext();

		String deserialized = serializationContext.key().read(serializationContext.key().write("foo"));

		assertThat(deserialized).isEqualTo("foo");
	}

	@Test // DATAREDIS-602
	public void shouldEncodeAndDecodeValue() {

		ReactiveSerializationContext<String, Long> serializationContext = createSerializationContext();

		long deserialized = serializationContext.value().read(serializationContext.value().write(42L));

		assertThat(deserialized).isEqualTo(42);
	}

	private ReactiveSerializationContext<String, Long> createSerializationContext() {

		return ReactiveSerializationContext.<String, Long> builder() //
				.key(new StringRedisSerializer()) //
				.value(ByteBuffer::getLong, value -> (ByteBuffer) ByteBuffer.allocate(8).putLong(value).flip()) //
				.hashKey(new StringRedisSerializer()) //
				.hashValue(new StringRedisSerializer()) //
				.build();
	}
}
