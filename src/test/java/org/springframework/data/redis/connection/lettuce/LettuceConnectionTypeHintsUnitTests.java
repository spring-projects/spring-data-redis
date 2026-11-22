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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.output.CommandOutput;

import org.junit.jupiter.api.Test;

import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link LettuceConnection.TypeHints}.
 *
 * @author arimu1
 */
class LettuceConnectionTypeHintsUnitTests {

	@Test // GH-3503
	void commandOutputWithoutRedisCodecConstructorThrowsIllegalArgumentException() {

		LettuceConnection.TypeHints typeHints = new LettuceConnection.TypeHints();

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> ReflectionTestUtils.invokeMethod(typeHints, "instantiateCommandOutput",
						OutputWithoutRedisCodecConstructor.class))
				.withMessageContaining("No constructor accepting RedisCodec");
	}

	private static final class OutputWithoutRedisCodecConstructor extends CommandOutput<byte[], byte[], byte[]> {

		private OutputWithoutRedisCodecConstructor() {
			super(null, null);
		}

	}

}
