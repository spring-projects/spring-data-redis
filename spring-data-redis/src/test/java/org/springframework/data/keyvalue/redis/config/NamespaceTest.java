/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.config;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.keyvalue.redis.core.StringRedisTemplate;
import org.springframework.data.keyvalue.redis.listener.RedisMessageListenerContainer;

/**
 * @author Costin Leau
 */
public class NamespaceTest {

	private GenericXmlApplicationContext ctx;

	@Before
	public void setUp() {
		ctx = new GenericXmlApplicationContext("/org/springframework/data/keyvalue/redis/config/namespace.xml");
	}

	@After
	public void tearDown() {
		if (ctx != null)
			ctx.destroy();
	}

	@Test
	public void testSanityTest() throws Exception {
		RedisMessageListenerContainer container = ctx.getBean(RedisMessageListenerContainer.class);
		assertTrue(container.isRunning());
		//Thread.sleep(TimeUnit.SECONDS.toMillis(1));
	}

	@Test
	public void testWithMessages() throws Exception {
		StringRedisTemplate template = ctx.getBean(StringRedisTemplate.class);
		template.convertAndSend("x1", "[X]test");
		template.convertAndSend("z1", "[Z]test");
		//Thread.sleep(TimeUnit.SECONDS.toMillis(5));
	}

	@Test
	public void testErrorHandler() throws Exception {
		StubErrorHandler handler = ctx.getBean(StubErrorHandler.class);
		
		int index = handler.throwables.size();
		StringRedisTemplate template = ctx.getBean(StringRedisTemplate.class);
		template.convertAndSend("exception", "test1");
		handler.throwables.pollLast(3, TimeUnit.SECONDS);
		assertEquals(index + 1, handler.throwables.size());
	}
}
