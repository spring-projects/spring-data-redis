/*
 * Copyright 2019-2021 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType;

import java.util.ArrayList;
import java.util.List;

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

	@Test //ISSUES #2055
	void shouldCreateBitCommandsWithChainingMethod(){

		BitFieldType type =  BitFieldType.unsigned(1);
		BitFieldSubCommands bitFieldSubCommands = BitFieldSubCommands.create()
				.get(type).valueAt(BitFieldSubCommands.Offset.offset(1))
				.get(type).valueAt(BitFieldSubCommands.Offset.offset(2))
				.set(type).valueAt(BitFieldSubCommands.Offset.offset(3)).to(1)
				.set(type).valueAt(BitFieldSubCommands.Offset.offset(4)).to(1)
				.incr(type).valueAt(BitFieldSubCommands.Offset.offset(5)).by(1);

		assertThat(bitFieldSubCommands.getSubCommands().size()).isEqualTo(5);
	}

	@Test //ISSUES #2055
	void shouldCreateBitCommandsWithNonChainingMethod(){

		List<BitFieldSubCommands.BitFieldSubCommand> subCommandList = new ArrayList<>();

		BitFieldType type =  BitFieldType.unsigned(1);

		subCommandList.add(new BitFieldSubCommands.BitFieldGet(type, BitFieldSubCommands.Offset.offset(1)));
		subCommandList.add(new BitFieldSubCommands.BitFieldSet(type, BitFieldSubCommands.Offset.offset(2),2));
		subCommandList.add(new BitFieldSubCommands.BitFieldIncrBy(type, BitFieldSubCommands.Offset.offset(3),3));
		subCommandList.add(new BitFieldSubCommands.BitFieldIncrBy(type, BitFieldSubCommands.Offset.offset(4),4,
				BitFieldSubCommands.BitFieldIncrBy.Overflow.FAIL));

		BitFieldSubCommands bitFieldSubCommands = new BitFieldSubCommands(subCommandList);

		assertThat(bitFieldSubCommands.getSubCommands().size()).isEqualTo(4);
	}
}
