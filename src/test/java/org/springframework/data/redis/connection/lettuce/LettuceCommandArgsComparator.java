/*
 * Copyright 2021-present the original author or authors.
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

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;

import org.assertj.core.api.Assertions;

/**
 * @author Christoph Strobl
 */
public class LettuceCommandArgsComparator {

	static void argsEqual(CompositeArgument args1, CompositeArgument args2) {

		CommandArgs<String, String> stringStringCommandArgs1 = new CommandArgs<>(StringCodec.UTF8);
		args1.build(stringStringCommandArgs1);

		CommandArgs<String, String> stringStringCommandArgs2 = new CommandArgs<>(StringCodec.UTF8);
		args2.build(stringStringCommandArgs2);

		Assertions.assertThat(stringStringCommandArgs1.toCommandString())
				.isEqualTo(stringStringCommandArgs2.toCommandString());
	}

	static LettuceCompositeArgumentAssert assertThatCommandArgument(CompositeArgument command) {
		return new LettuceCompositeArgumentAssert() {

			@Override
			public LettuceCompositeArgumentAssert isEqualTo(CompositeArgument expected) {

				argsEqual(command, expected);
				return this;
			}
		};
	}

	interface LettuceCompositeArgumentAssert {
		LettuceCompositeArgumentAssert isEqualTo(CompositeArgument command);
	}
}
