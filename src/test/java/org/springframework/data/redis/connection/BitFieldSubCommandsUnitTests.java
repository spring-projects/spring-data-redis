/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType;

/**
 * Unit tests for {@link BitFieldSubCommands}.
 *
 * @author Mark Paluch
 */
public class BitFieldSubCommandsUnitTests {

	@Test // DATAREDIS-971
	public void shouldCreateSignedBitFieldType() {

		BitFieldType type = BitFieldType.signed(10);

		assertThat(type.isSigned()).isTrue();
		assertThat(type.getBits()).isEqualTo(10);
	}

	@Test // DATAREDIS-971
	public void shouldCreateUnsignedBitFieldType() {

		BitFieldType type = BitFieldType.unsigned(10);

		assertThat(type.isSigned()).isFalse();
		assertThat(type.getBits()).isEqualTo(10);
	}
}
