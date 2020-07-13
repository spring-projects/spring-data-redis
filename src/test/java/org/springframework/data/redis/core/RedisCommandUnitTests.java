/*
 * Copyright 2014-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RedisCommand}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Oscar Cai
 */
public class RedisCommandUnitTests {

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectly() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("setconfig")).isTrue();
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCase() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("SetConfig")).isTrue();
	}

	@Test // DATAREDIS-73
	public void shouldNotThrowExceptionWhenUsingNullKeyForRepresentationCheck() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy(null)).isFalse();
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("setconfig")).isEqualTo(RedisCommand.CONFIG_SET);
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCaseViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("SetConfig")).isEqualTo(RedisCommand.CONFIG_SET);
	}

	@Test // DATAREDIS-73
	public void shouldReturnUnknownCommandForUnknownCommandString() {
		assertThat(RedisCommand.failsafeCommandLookup("strangecommand")).isEqualTo(RedisCommand.UNKNOWN);
	}

	@Test // DATAREDIS-73, DATAREDIS-972, DATAREDIS-1013
	public void shouldNotThrowExceptionOnValidArgumentCount() {

		RedisCommand.AUTH.validateArgumentCount(1);
		RedisCommand.ZADD.validateArgumentCount(3);
		RedisCommand.ZADD.validateArgumentCount(4);
		RedisCommand.ZADD.validateArgumentCount(5);
		RedisCommand.ZADD.validateArgumentCount(100);
		RedisCommand.SELECT.validateArgumentCount(1);
	}

	@Test // DATAREDIS-822
	public void shouldConsiderMinMaxArguments() {

		RedisCommand.BITPOS.validateArgumentCount(2);
		RedisCommand.BITPOS.validateArgumentCount(3);
		RedisCommand.BITPOS.validateArgumentCount(4);
	}

	@Test // DATAREDIS-822
	public void shouldReportArgumentMismatchIfMaxArgumentsExceeded() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("SELECT command requires 1 argument");

		RedisCommand.SELECT.validateArgumentCount(0);
	}

	@Test // DATAREDIS-73
	public void shouldThrowExceptionOnInvalidArgumentCountWhenExpectedExactMatch() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("AUTH command requires 1 argument");

		RedisCommand.AUTH.validateArgumentCount(2);
	}

	@Test // DATAREDIS-73
	public void shouldThrowExceptionOnInvalidArgumentCountForDelWhenExpectedMinimalMatch() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("DEL command requires at least 1 argument");

		RedisCommand.DEL.validateArgumentCount(0);
	}

	@Test // DATAREDIS-972
	public void shouldThrowExceptionOnInvalidArgumentCountForZaddWhenExpectedMinimalMatch() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ZADD command requires at least 3 arguments");

		RedisCommand.ZADD.validateArgumentCount(2);
	}
}
