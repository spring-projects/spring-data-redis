/*
 * Copyright 2014 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
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

	@Test // DATAREDIS-73
	public void shouldNotThrowExceptionOnValidArgumentCount() {
		RedisCommand.AUTH.validateArgumentCount(1);
	}

	@Test // DATAREDIS-73
	public void shouldThrowExceptionOnInvalidArgumentCountWhenExpectedExcatMatch() {

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
