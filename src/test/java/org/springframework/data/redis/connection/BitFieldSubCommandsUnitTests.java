/*
 * Copyright 2019-present the original author or authors.
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
import static org.springframework.data.redis.connection.BitFieldSubCommands.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BitFieldSubCommands}.
 *
 * @author Mark Paluch
 * @author Yanam
 */
class BitFieldSubCommandsUnitTests {

	@Test // DATAREDIS-971
	void shouldCreateSignedBitFieldType() {

		BitFieldType type = BitFieldType.signed(10);

		assertThat(type.isSigned()).isTrue();
		assertThat(type.getBits()).isEqualTo(10);
	}

	@Test // DATAREDIS-971
	void shouldCreateUnsignedBitFieldType() {

		BitFieldType type = BitFieldType.unsigned(10);

		assertThat(type.isSigned()).isFalse();
		assertThat(type.getBits()).isEqualTo(10);
	}

	@Test // GH-2055
	void shouldCreateBitCommandsWithChainingMethod() {

		BitFieldType type = BitFieldType.unsigned(1);
		BitFieldSubCommands bitFieldSubCommands = BitFieldSubCommands.create().get(type).valueAt(Offset.offset(1)).get(type)
				.valueAt(Offset.offset(2)).set(type).valueAt(Offset.offset(3)).to(1).set(type).valueAt(Offset.offset(4)).to(1)
				.incr(type).valueAt(Offset.offset(5)).by(1);

		assertThat(bitFieldSubCommands.getSubCommands()).hasSize(5);
	}

	@Test // GH-2055
	void shouldCreateEqualObjects() {

		BitFieldType type = BitFieldType.unsigned(1);

		BitFieldSubCommands createdWithBuilder = BitFieldSubCommands.create() //
				.get(type).valueAt(Offset.offset(2)) //
				.set(type).valueAt(Offset.offset(3)).to(1) //
				.incr(type).valueAt(Offset.offset(5)).by(1);

		BitFieldSubCommand subGetCommand = BitFieldGet.create(type, Offset.offset(2));
		BitFieldSubCommand subSetCommand = BitFieldSet.create(type, Offset.offset(3), 1);
		BitFieldSubCommand subIncrByCommand = BitFieldIncrBy.create(type, Offset.offset(5), 1);

		BitFieldSubCommands createdWithCreate = BitFieldSubCommands.create(subGetCommand, subSetCommand, subIncrByCommand);

		assertThat(createdWithBuilder).isEqualTo(createdWithCreate).hasSameHashCodeAs(createdWithCreate);
	}

	@Test // GH-2055
	void shouldCreateBitCommandsWithNonChainingMethod() {

		BitFieldType type = BitFieldType.unsigned(1);
		Offset offset = Offset.offset(1);

		BitFieldSubCommand subGetCommand = BitFieldGet.create(type, offset);
		BitFieldSubCommand subSetCommand = BitFieldSet.create(type, offset, 1);
		BitFieldSubCommand subIncrByCommand = BitFieldIncrBy.create(type, offset, 1);
		BitFieldSubCommand subIncrByCommand2 = BitFieldIncrBy.create(type, offset, 1, BitFieldIncrBy.Overflow.FAIL);

		BitFieldSubCommands bitFieldSubCommands = BitFieldSubCommands.create(subGetCommand, subSetCommand, subIncrByCommand,
				subIncrByCommand2);

		assertThat(bitFieldSubCommands.getSubCommands()).hasSize(4);
	}
}
