/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core.script;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;

/**
 * Test of {@link DefaultRedisScript}
 * 
 * @author Jennifer Hickey
 */
public class DefaultRedisScriptTests {

	@Test
	public void testGetSha1() {
		StaticScriptSource script = new StaticScriptSource("return KEYS[1]");
		DefaultRedisScript<String> redisScript = new DefaultRedisScript<String>();
		redisScript.setScriptSource(script);
		redisScript.setResultType(String.class);
		String sha1 = redisScript.getSha1();
		// Ensure multiple calls return same sha
		assertThat(redisScript.getSha1()).isEqualTo(sha1);
		script.setScript("return KEYS[2]");
		// Sha should now be different as script text has changed
		assertThat(sha1.equals(redisScript.getSha1())).isFalse();
	}

	@Test
	public void testGetScriptAsString() {
		DefaultRedisScript<String> redisScript = new DefaultRedisScript<String>();
		redisScript.setScriptText("return ARGS[1]");
		redisScript.setResultType(String.class);
		assertThat(redisScript.getScriptAsString()).isEqualTo("return ARGS[1]");
	}

	@Test(expected = ScriptingException.class)
	public void testGetScriptAsStringError() {
		DefaultRedisScript<Long> redisScript = new DefaultRedisScript<Long>();
		redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("nonexistent")));
		redisScript.setResultType(Long.class);
		redisScript.getScriptAsString();
	}

	@Test(expected = IllegalArgumentException.class)
	public void initializeWithNoScript() throws Exception {
		DefaultRedisScript<Long> redisScript = new DefaultRedisScript<Long>();
		redisScript.afterPropertiesSet();
	}
}
