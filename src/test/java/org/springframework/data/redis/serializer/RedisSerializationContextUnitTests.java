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
 * Unit tests for {@link RedisSerializationContext}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class RedisSerializationContextUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfKeySerializerIsNotSet() {

		RedisSerializationContext.<String, String> newSerializationContext() //
				.value(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfValueSerializerIsNotSet() {

		RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfHashKeySerializerIsNotSet() {

		RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
				.build();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void shouldRejectBuildIfHashValueSerializerIsNotSet() {

		RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.build();
	}

	@Test // DATAREDIS-602
	public void shouldUseDefaultIfSet() {

		RedisSerializationContext.<String, String> newSerializationContext(StringRedisSerializer.UTF_8)
				.key(new GenericToStringSerializer(Long.class))//
				.build();
	}

	@Test // DATAREDIS-602
	public void shouldBuildSerializationContext() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		assertThat(serializationContext.getKeySerializationPair()).isNotNull();
		assertThat(serializationContext.getValueSerializationPair()).isNotNull();
		assertThat(serializationContext.getHashKeySerializationPair()).isNotNull();
		assertThat(serializationContext.getHashValueSerializationPair()).isNotNull();
		assertThat(serializationContext.getStringSerializationPair()).isNotNull();
	}

	@Test // DATAREDIS-602
	public void shouldEncodeAndDecodeKey() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		String deserialized = serializationContext.getKeySerializationPair()
				.read(serializationContext.getKeySerializationPair().write("foo"));

		assertThat(deserialized).isEqualTo("foo");
	}

	@Test // DATAREDIS-602
	public void shouldEncodeAndDecodeValue() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		long deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair().write(42L));

		assertThat(deserialized).isEqualTo(42);
	}

	private RedisSerializationContext<String, Long> createSerializationContext() {

		return RedisSerializationContext.<String, Long> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(ByteBuffer::getLong, value -> (ByteBuffer) ByteBuffer.allocate(8).putLong(value).flip()) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
				.build();
	}
}
