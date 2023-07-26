/*
 * Copyright 2017-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

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
	void shouldUseDefaultIfSet() {

		RedisSerializationContext.<String, String> newSerializationContext(StringRedisSerializer.UTF_8)
				.key(new GenericToStringSerializer(Long.class))//
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

		RedisSerializationContext<ByteBuffer, ByteBuffer> serializationContext = RedisSerializationContext
				.byteBuffer();

		ByteBuffer deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair()
						.write(ByteBuffer.wrap("hello".getBytes())));

		assertThat(deserialized).isEqualTo(ByteBuffer.wrap("hello".getBytes()));
	}

	@Test // DATAREDIS-1000
	void shouldEncodeAndDecodeByteArrayValue() {

		RedisSerializationContext<byte[], byte[]> serializationContext = RedisSerializationContext
				.byteArray();

		byte[] deserialized = serializationContext.getValueSerializationPair()
				.read(serializationContext.getValueSerializationPair()
						.write("hello".getBytes()));

		assertThat(deserialized).isEqualTo("hello".getBytes());
	}

	@Test
	void shouldEncodeAndDecodeUtf8StringValue() {
		RedisSerializationContext<String, String> serializationContext = RedisSerializationContext
				.<String, String>newSerializationContext(StringRedisSerializer.UTF_8)
				.string(StringRedisSerializer.UTF_8)
				.build();
		RedisSerializationContext.SerializationPair<String> serializationPair = serializationContext.getStringSerializationPair();

		assertThat(serializationPair.write("üßØ"))
				.isEqualTo(ByteBuffer.wrap("üßØ".getBytes(StandardCharsets.UTF_8)));
		assertThat(serializationPair.read(ByteBuffer.wrap("üßØ".getBytes(StandardCharsets.UTF_8))))
				.isEqualTo("üßØ");
	}

	@Test
	void shouldEncodeAndDecodeAsciiStringValue() {
		RedisSerializationContext<String, String> serializationContext = RedisSerializationContext
				.<String, String>newSerializationContext(StringRedisSerializer.US_ASCII)
				.string(StringRedisSerializer.US_ASCII)
				.build();
		RedisSerializationContext.SerializationPair<String> serializationPair = serializationContext.getStringSerializationPair();

		assertThat(serializationPair.write("üßØ"))
				.isEqualTo(ByteBuffer.wrap("???".getBytes(StandardCharsets.US_ASCII)));
		assertThat(serializationPair.read(ByteBuffer.wrap("üßØ".getBytes(StandardCharsets.US_ASCII))))
				.isEqualTo("???");
	}

	@Test
	void shouldEncodeAndDecodeIso88591StringValue() {
		RedisSerializationContext<String, String> serializationContext = RedisSerializationContext
				.<String, String>newSerializationContext(StringRedisSerializer.ISO_8859_1)
				.string(StringRedisSerializer.ISO_8859_1)
				.build();
		RedisSerializationContext.SerializationPair<String> serializationPair = serializationContext.getStringSerializationPair();

		assertThat(serializationPair.write("üßØ"))
				.isEqualTo(ByteBuffer.wrap("üßØ".getBytes(StandardCharsets.ISO_8859_1)));
		assertThat(serializationPair.read(ByteBuffer.wrap("üßØ".getBytes(StandardCharsets.ISO_8859_1))))
				.isEqualTo("üßØ");
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
