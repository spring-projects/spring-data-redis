/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;

import io.lettuce.core.ScriptOutputType;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.ReturnType;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceReactiveScriptingCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-683
	public void scriptExistsShouldReturnState() {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		StepVerifier.create(connection.scriptingCommands().scriptExists(Arrays.asList("foo", sha1))) //
				.expectNext(false) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683
	public void scriptFlushShouldRemoveScripts() {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		StepVerifier.create(connection.scriptingCommands().scriptExists(sha1)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(connection.scriptingCommands().scriptFlush()) //
				.expectNext("OK") //
				.verifyComplete();

		StepVerifier.create(connection.scriptingCommands().scriptExists(sha1)) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683
	public void evalShaShouldReturnKey() {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		String sha1 = nativeCommands.scriptLoad("return KEYS[1]");

		StepVerifier
				.create(connection.scriptingCommands().evalSha(sha1, ReturnType.VALUE, 2, SAME_SLOT_KEY_1_BBUFFER.duplicate(),
						SAME_SLOT_KEY_2_BBUFFER.duplicate())) //
				.expectNext(SAME_SLOT_KEY_1_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683, DATAREDIS-711
	public void evalShaShouldReturnMulti() {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		String sha1 = nativeCommands.scriptLoad("return {KEYS[1],ARGV[1]}");

		StepVerifier
				.create(connection.scriptingCommands().evalSha(sha1, ReturnType.MULTI, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate(),
						SAME_SLOT_KEY_2_BBUFFER.duplicate())) //
				.expectNext(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683
	public void evalShaShouldFail() {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		StepVerifier
				.create(connection.scriptingCommands().evalSha("foo", ReturnType.VALUE, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate())) //
				.expectError(RedisSystemException.class) //
				.verify();
	}

	@Test // DATAREDIS-683
	public void evalShouldReturnStatus() {

		ByteBuffer script = wrap(String.format("return redis.call('set','%s','ghk')", SAME_SLOT_KEY_1));

		StepVerifier
				.create(connection.scriptingCommands().eval(script, ReturnType.STATUS, 1, SAME_SLOT_KEY_1_BBUFFER.duplicate())) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-683
	public void evalShouldReturnBooleanFalse() {

		ByteBuffer script = wrap("return false");

		StepVerifier.create(connection.scriptingCommands().eval(script, ReturnType.BOOLEAN, 0)) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683, DATAREDIS-711
	public void evalShouldReturnMultiNumbers() {

		ByteBuffer script = wrap("return {1,2}");

		StepVerifier.create(connection.scriptingCommands().eval(script, ReturnType.MULTI, 0)) //
				.expectNext(Arrays.asList(1L, 2L)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-683
	public void evalShouldFailWithScriptError() {

		ByteBuffer script = wrap("return {1,2");

		StepVerifier.create(connection.scriptingCommands().eval(script, ReturnType.MULTI, 0)) //
				.expectError(RedisSystemException.class) //
				.verify();
	}

	@Test // DATAREDIS-683
	public void scriptKillShouldKillScripts() throws Exception {

		assumeFalse(connection instanceof ReactiveRedisClusterConnection);

		AtomicBoolean scriptDead = new AtomicBoolean(false);
		CountDownLatch sync = new CountDownLatch(1);
		Thread th = new Thread(() -> {
			try {
				sync.countDown();
				nativeCommands.eval("local time=1 while time < 10000000000 do time=time+1 end", ScriptOutputType.BOOLEAN);
			} catch (Exception e) {
				scriptDead.set(true);
			}
		});
		th.start();
		sync.await(2, TimeUnit.SECONDS);
		Thread.sleep(200);

		StepVerifier.create(connection.scriptingCommands().scriptKill()).expectNext("OK").verifyComplete();

		assertTrue(waitFor(scriptDead::get, 3000L));
	}

	private static ByteBuffer wrap(String content) {
		return ByteBuffer.wrap(content.getBytes());
	}
}
