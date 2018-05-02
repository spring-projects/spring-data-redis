/*
 * Copyright 2014-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RedisCommand}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class RedisCommandUnitTests {

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectly() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("setconfig"), equalTo(true));
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCase() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy("SetConfig"), equalTo(true));
	}

	@Test // DATAREDIS-73
	public void shouldNotThrowExceptionWhenUsingNullKeyForRepresentationCheck() {
		assertThat(RedisCommand.CONFIG_SET.isRepresentedBy(null), equalTo(false));
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("setconfig"), is(RedisCommand.CONFIG_SET));
	}

	@Test // DATAREDIS-73
	public void shouldIdentifyAliasCorrectlyWhenNamePassedInMixedCaseViaLookup() {
		assertThat(RedisCommand.failsafeCommandLookup("SetConfig"), is(RedisCommand.CONFIG_SET));
	}

	@Test // DATAREDIS-73
	public void shouldReturnUnknownCommandForUnknownCommandString() {
		assertThat(RedisCommand.failsafeCommandLookup("strangecommand"), is(RedisCommand.UNKNOWN));
	}

	@Test // DATAREDIS-73
	public void shouldNotThrowExceptionOnValidArgumentCount() {
		RedisCommand.AUTH.validateArgumentCount(1);
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
		expectedException.expectMessage("BITPOS command requires at most 4 arguments");

		RedisCommand.BITPOS.validateArgumentCount(5);
	}

	@Test // DATAREDIS-73
	public void shouldThrowExceptionOnInvalidArgumentCountWhenExpectedExactMatch() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("AUTH command requires 1 arguments");
		RedisCommand.AUTH.validateArgumentCount(2);
	}

	@Test // DATAREDIS-73
	public void shouldThrowExceptionOnInvalidArgumentCountWhenExpectedMinimalMatch() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("DEL command requires at least 1 arguments");
		RedisCommand.DEL.validateArgumentCount(0);
	}
}
