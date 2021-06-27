package org.springframework.data.redis.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Yongjoon Park
 */
class ByteUtilsTest {

	@Test
	void lastIndexOf() {
		byte[] haystack = {1, 2, 3, 4, 5};
		byte needle = (byte) 2;
		assertThat(ByteUtils.lastIndexOf(haystack, needle, 3)).isEqualTo(1);
		assertThat(ByteUtils.lastIndexOf(haystack, needle, haystack.length + 1)).isEqualTo(-1);
		assertThat(ByteUtils.lastIndexOf(haystack, needle, 0)).isEqualTo(-1);
	}
}
