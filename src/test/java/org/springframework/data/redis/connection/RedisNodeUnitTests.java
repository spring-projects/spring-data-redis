package org.springframework.data.redis.connection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

public class RedisNodeUnitTests {
	private static final int DEFAULT_PORT = 6379;

	@Test // GH-2928
	void shouldParseIPv4AddressWithPort() {
		RedisNode node = RedisNode.fromString("127.0.0.1:6379");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6379);
	}

	@Test // GH-2928
	void shouldParseIPv6AddressWithPort() {
		RedisNode node = RedisNode.fromString("[aaaa:bbbb::dddd:eeee]:6379");
		assertThat(node.getHost()).isEqualTo("aaaa:bbbb::dddd:eeee");
		assertThat(node.getPort()).isEqualTo(6379);
	}

	@Test // GH-2928
	void shouldParseBareHostnameWithPort() {
		RedisNode node = RedisNode.fromString("my.redis.server:6379");
		assertThat(node.getHost()).isEqualTo("my.redis.server");
		assertThat(node.getPort()).isEqualTo(6379);
	}

	@Test // GH-2928
	void shouldThrowExceptionForInvalidPort() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> RedisNode.fromString("127.0.0.1:invalidPort"));
	}

	@Test // GH-2928
	void shouldParseBareIPv6WithoutPort() {
		RedisNode node = RedisNode.fromString("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
		assertThat(node.getHost()).isEqualTo("2001:0db8:85a3:0000:0000:8a2e:0370");
		assertThat(node.getPort()).isEqualTo(7334);
	}

	@Test // GH-2928
	void shouldParseBareHostnameWithoutPort() {
		RedisNode node = RedisNode.fromString("my.redis.server");
		assertThat(node.getHost()).isEqualTo("my.redis.server");
		assertThat(node.getPort()).isEqualTo(DEFAULT_PORT);
	}
}

