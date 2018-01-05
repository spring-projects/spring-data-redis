/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.config;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Costin Leau
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("namespace.xml")
@ProfileValueSourceConfiguration
public class NamespaceTest {

	@Autowired private RedisMessageListenerContainer container;

	@Autowired private StringRedisTemplate template;

	@Autowired private StubErrorHandler handler;

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testSanityTest() throws Exception {
		assertTrue(container.isRunning());
		Thread.sleep(TimeUnit.SECONDS.toMillis(8));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testWithMessages() throws Exception {
		template.convertAndSend("x1", "[X]test");
		template.convertAndSend("z1", "[Z]test");
		Thread.sleep(TimeUnit.SECONDS.toMillis(8));
	}

	public void testErrorHandler() throws Exception {
		int index = handler.throwables.size();
		template.convertAndSend("exception", "test1");
		handler.throwables.pollLast(3, TimeUnit.SECONDS);
		assertEquals(index + 1, handler.throwables.size());
	}
}
