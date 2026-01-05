/*
 * Copyright 2017-present the original author or authors.
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

import static org.assertj.core.api.Assumptions.*;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceReactiveScriptingCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveScriptingCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void scriptExistsShouldReturnState() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		connection.scriptingCommands().scriptExists(Arrays.asList("foo", sha1)).as(StepVerifier::create) //
				.expectNext(false) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void scriptFlushShouldRemoveScripts() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		connection.scriptingCommands().scriptExists(sha1).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		connection.scriptingCommands().scriptFlush().as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();

		connection.scriptingCommands().scriptExists(sha1).as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void evalShaShouldReturnKey() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		connection.scriptingCommands()
				.evalSha(sha1, ReturnType.VALUE, 2, SAME_SLOT_KEY_1_BBUFFER.duplicate(), SAME_SLOT_KEY_2_BBUFFER.duplicate())
				.as(StepVerifier::create) //
				.expectNext(SAME_SLOT_KEY_1_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683, DATAREDIS-711
	void evalShaShouldReturnMulti() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		String sha1 = nativeCommands.scriptLoad("return {KEYS[1],ARGV[1]}");

		connection.scriptingCommands()
				.evalSha(sha1, ReturnType.MULTI, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate(), SAME_SLOT_KEY_2_BBUFFER.duplicate())
				.as(StepVerifier::create) //
				.expectNext(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void evalShaShouldFail() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		connection.scriptingCommands().evalSha("foo", ReturnType.VALUE, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate())
				.as(StepVerifier::create) //
				.expectError(RedisSystemException.class) //
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void evalShouldReturnStatus() {

		ByteBuffer script = wrap("return redis.call('set','%s','ghk')".formatted(SAME_SLOT_KEY_1));

		connection.scriptingCommands().eval(script, ReturnType.STATUS, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate())
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void evalShouldReturnBooleanFalse() {

		ByteBuffer script = wrap("return false");

		connection.scriptingCommands().eval(script, ReturnType.BOOLEAN, 0).as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683, DATAREDIS-711
	void evalShouldReturnMultiNumbers() {

		ByteBuffer script = wrap("return {1,2}");

		connection.scriptingCommands().eval(script, ReturnType.MULTI, 0).as(StepVerifier::create) //
				.expectNext(Arrays.asList(1L, 2L)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-683
	void evalShouldFailWithScriptError() {

		ByteBuffer script = wrap("return {1,2");

		connection.scriptingCommands().eval(script, ReturnType.MULTI, 0).as(StepVerifier::create) //
				.expectError(RedisSystemException.class) //
				.verify();
	}

	private static ByteBuffer wrap(String content) {
		return ByteBuffer.wrap(content.getBytes());
	}
}
