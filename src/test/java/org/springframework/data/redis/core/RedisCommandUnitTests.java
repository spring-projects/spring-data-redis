/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RedisCommand}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Oscar Cai
 * @author John Blum
 */
class RedisCommandUnitTests {

	@Test // DATAREDIS-73
	void shouldIdentifyAliasCorrectly() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("setconfig")).isTrue();
	}

	@Test // DATAREDIS-73
	void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCase() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("SetConfig")).isTrue();
	}

	@Test // DATAREDIS-73
	void shouldNotThrowExceptionWhenUsingNullKeyForRepresentationCheck() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy(null)).isFalse();
	}

	@Test // DATAREDIS-73
	void shouldIdentifyAliasCorrectlyViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("setconfig")).isEqualTo(RedisCommand.CONFIG_SET);
	}

	@Test // DATAREDIS-73
	void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCaseViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("SetConfig")).isEqualTo(RedisCommand.CONFIG_SET);
	}

	@Test // DATAREDIS-73
	void shouldReturnUnknownCommandForUnknownCommandString() {
		assertThat(RedisCommand.failsafeCommandLookup("strangecommand")).isEqualTo(RedisCommand.UNKNOWN);
	}

	@Test // DATAREDIS-73, DATAREDIS-972, DATAREDIS-1013
	void shouldNotThrowExceptionOnValidArgumentCount() {

		RedisCommand.AUTH.validateArgumentCount(1);
		RedisCommand.ZADD.validateArgumentCount(3);
		RedisCommand.ZADD.validateArgumentCount(4);
		RedisCommand.ZADD.validateArgumentCount(5);
		RedisCommand.ZADD.validateArgumentCount(100);
		RedisCommand.SELECT.validateArgumentCount(1);
	}

	@Test // DATAREDIS-822
	void shouldConsiderMinMaxArguments() {

		RedisCommand.BITPOS.validateArgumentCount(2);
		RedisCommand.BITPOS.validateArgumentCount(3);
		RedisCommand.BITPOS.validateArgumentCount(4);
	}

	@Test // DATAREDIS-822
	void shouldReportArgumentMismatchIfMaxArgumentsExceeded() {
		assertThatIllegalArgumentException().isThrownBy(() -> RedisCommand.SELECT.validateArgumentCount(0))
				.withMessageContaining("SELECT command requires 1 argument");
	}

	@Test // DATAREDIS-73
	void shouldThrowExceptionOnInvalidArgumentCountWhenExpectedExactMatch() {
		assertThatIllegalArgumentException().isThrownBy(() -> RedisCommand.AUTH.validateArgumentCount(2))
				.withMessageContaining("AUTH command requires 1 argument");
	}

	@Test // DATAREDIS-73
	void shouldThrowExceptionOnInvalidArgumentCountForDelWhenExpectedMinimalMatch() {
		assertThatIllegalArgumentException().isThrownBy(() -> RedisCommand.DEL.validateArgumentCount(0))
				.withMessageContaining("DEL command requires at least 1 argument");
	}

	@Test // DATAREDIS-972
	void shouldThrowExceptionOnInvalidArgumentCountForZaddWhenExpectedMinimalMatch() {
		assertThatIllegalArgumentException().isThrownBy(() -> RedisCommand.ZADD.validateArgumentCount(2))
				.withMessageContaining("ZADD command requires at least 3 arguments");
	}

	@Test // GH-2646
	void commandRequiresArgumentsIsCorrect() {

		Arrays.stream(RedisCommand.values()).forEach(command ->
			assertThat(command.requiresArguments())
				.describedAs("Redis command [%s] failed required arguments check", command)
				.isEqualTo(command.minArgs > 0));
	}

	@Test // GH-2646
	void commandRequiresExactNumberOfArgumentsIsCorrect() {

		Arrays.stream(RedisCommand.values()).forEach(command ->
			assertThat(command.requiresExactNumberOfArguments())
				.describedAs("Redis command [%s] failed requires exact arguments check")
				.isEqualTo(command.minArgs == command.maxArgs));
	}
}
