/*
 * Copyright 2024-present the original author or authors.
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

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveStreamCommands.PendingRecordsCommand;

/**
 * Unit tests for {@link ReactiveStreamCommands}.
 *
 * @author jinkshower
 */
class ReactiveStreamCommandsUnitTests {

	@Test // GH-2982
	void pendingRecordsCommandRangeShouldThrowExceptionWhenRangeIsNull() {

		ByteBuffer key = ByteBuffer.wrap("my-stream".getBytes());
		String groupName = "my-group";

		PendingRecordsCommand command = PendingRecordsCommand.pending(key, groupName);

		assertThatIllegalArgumentException().isThrownBy(() -> command.range(null, 10L));
	}

	@Test // GH-2982
	void pendingRecordsCommandRangeShouldThrowExceptionWhenCountIsNegative() {

		ByteBuffer key = ByteBuffer.wrap("my-stream".getBytes());
		String groupName = "my-group";

		PendingRecordsCommand command = PendingRecordsCommand.pending(key, groupName);
		Range<?> range = Range.closed("0", "10");

		assertThatIllegalArgumentException().isThrownBy(() -> command.range(range, -1L));
	}
}
