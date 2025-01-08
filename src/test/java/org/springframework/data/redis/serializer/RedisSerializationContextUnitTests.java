/*
 * Copyright 2017-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RedisSerializationContext}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Zhou KQ
 */
class RedisSerializationContextUnitTests {

	@Test // DATAREDIS-602
	void shouldRejectBuildIfKeySerializerIsNotSet() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> RedisSerializationContext.<String, String> newSerializationContext() //
				.value(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
						.build());
	}

	@Test // DATAREDIS-602
	void shouldRejectBuildIfValueSerializerIsNotSet() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
						.build());
	}

	@Test // DATAREDIS-602
	void shouldRejectBuildIfHashKeySerializerIsNotSet() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
						.build());
	}

	@Test // DATAREDIS-602
	void shouldRejectBuildIfHashValueSerializerIsNotSet() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> RedisSerializationContext.<String, String> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(StringRedisSerializer.UTF_8) //
				.hashKey(StringRedisSerializer.UTF_8) //
						.build());
	}

	@Test // DATAREDIS-602
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void shouldUseDefaultIfSet() {

		RedisSerializationContext.<String, String>newSerializationContext(StringRedisSerializer.UTF_8)
				.key(new GenericToStringSerializer(Long.class)) //
				.build();
	}

	@Test // DATAREDIS-602
	void shouldBuildSerializationContext() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		assertThat(serializationContext.getKeySerializationPair()).isNotNull();
		assertThat(serializationContext.getValueSerializationPair()).isNotNull();
		assertThat(serializationContext.getHashKeySerializationPair()).isNotNull();
		assertThat(serializationContext.getHashValueSerializationPair()).isNotNull();
		assertThat(serializationContext.getStringSerializationPair()).isNotNull();
	}

	@Test // DATAREDIS-602
	void shouldEncodeAndDecodeKey() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		String deserialized = serializationContext.getKeySerializationPair()
				.read(serializationContext.getKeySerializationPair().write("foo"));

		assertThat(deserialized).isEqualTo("foo");
	}

	@Test // DATAREDIS-602
	void shouldEncodeAndDecodeValue() {

		RedisSerializationContext<String, Long> serializationContext = createSerializationContext();

		long deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair().write(42L));

		assertThat(deserialized).isEqualTo(42);
	}

	@Test // DATAREDIS-1000
	void shouldEncodeAndDecodeRawByteBufferValue() {

		RedisSerializationContext<ByteBuffer, ByteBuffer> serializationContext = RedisSerializationContext.byteBuffer();

		ByteBuffer deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair()
						.write(ByteBuffer.wrap("hello".getBytes())));

		assertThat(deserialized).isEqualTo(ByteBuffer.wrap("hello".getBytes()));
	}

	@Test // DATAREDIS-1000
	void shouldEncodeAndDecodeByteArrayValue() {

		RedisSerializationContext<byte[], byte[]> serializationContext = RedisSerializationContext.byteArray();

		byte[] deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair()
						.write("hello".getBytes()));

		assertThat(deserialized).isEqualTo("hello".getBytes());
	}

	@Test // GH-2651
	void shouldEncodeAndDecodeUtf8StringValue() {

		RedisSerializationContext.SerializationPair<String> serializationPair =
				buildStringSerializationContext(StringRedisSerializer.UTF_8).getStringSerializationPair();

		assertThat(serializationPair.write("üßØ")).isEqualTo(StandardCharsets.UTF_8.encode("üßØ"));
		assertThat(serializationPair.read(StandardCharsets.UTF_8.encode("üßØ"))).isEqualTo("üßØ");
	}

	@Test // GH-2651
	void shouldEncodeAndDecodeAsciiStringValue() {

		RedisSerializationContext.SerializationPair<String> serializationPair =
				buildStringSerializationContext(StringRedisSerializer.US_ASCII).getStringSerializationPair();

		assertThat(serializationPair.write("üßØ")).isEqualTo(StandardCharsets.US_ASCII.encode("???"));
		assertThat(serializationPair.read(StandardCharsets.US_ASCII.encode("üßØ"))).isEqualTo("???");
	}

	@Test  // GH-2651
	void shouldEncodeAndDecodeIso88591StringValue() {

		RedisSerializationContext.SerializationPair<String> serializationPair =
				buildStringSerializationContext(StringRedisSerializer.ISO_8859_1).getStringSerializationPair();

		assertThat(serializationPair.write("üßØ")).isEqualTo(StandardCharsets.ISO_8859_1.encode("üßØ"));
		assertThat(serializationPair.read(StandardCharsets.ISO_8859_1.encode("üßØ"))).isEqualTo("üßØ");
	}

	private RedisSerializationContext<String, Long> createSerializationContext() {

		return RedisSerializationContext.<String, Long> newSerializationContext() //
				.key(StringRedisSerializer.UTF_8) //
				.value(ByteBuffer::getLong, value -> ByteBuffer.allocate(8).putLong(value).flip()) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(StringRedisSerializer.UTF_8) //
				.build();
	}

	private RedisSerializationContext<String, String> buildStringSerializationContext(
			RedisSerializer<String> stringSerializer) {

		return RedisSerializationContext.<String, String>newSerializationContext(stringSerializer)
				.string(stringSerializer)
				.build();
	}
}
