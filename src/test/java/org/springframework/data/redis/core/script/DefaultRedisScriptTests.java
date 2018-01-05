/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.core.script;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;

/**
 * Test of {@link DefaultRedisScript}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
public class DefaultRedisScriptTests {

	@Test
	public void testGetSha1() {

		StaticScriptSource script = new StaticScriptSource("return KEYS[1]");
		DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
		redisScript.setScriptSource(script);
		redisScript.setResultType(String.class);
		String sha1 = redisScript.getSha1();
		// Ensure multiple calls return same sha
		assertEquals(sha1, redisScript.getSha1());
		script.setScript("return KEYS[2]");
		// Sha should now be different as script text has changed
		assertFalse(sha1.equals(redisScript.getSha1()));
	}

	@Test
	public void testGetScriptAsString() {

		DefaultRedisScript<String> redisScript = new DefaultRedisScript<>();
		redisScript.setScriptText("return ARGS[1]");
		redisScript.setResultType(String.class);
		assertEquals("return ARGS[1]", redisScript.getScriptAsString());
	}

	@Test(expected = ScriptingException.class)
	public void testGetScriptAsStringError() {

		DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
		redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("nonexistent")));
		redisScript.setResultType(Long.class);
		redisScript.getScriptAsString();
	}

	@Test(expected = IllegalStateException.class)
	public void initializeWithNoScript() throws Exception {

		DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
		redisScript.afterPropertiesSet();
	}
}
