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

		assertThatThrownBy(() -> command.range(null, 10L)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-2982
	void pendingRecordsCommandRangeShouldThrowExceptionWhenCountIsNegative() {

		ByteBuffer key = ByteBuffer.wrap("my-stream".getBytes());
		String groupName = "my-group";

		PendingRecordsCommand command = PendingRecordsCommand.pending(key, groupName);
		Range<?> range = Range.closed("0", "10");

		assertThatThrownBy(() -> command.range(range, -1L)).isInstanceOf(IllegalArgumentException.class);
	}
}
