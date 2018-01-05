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
package org.springframework.data.redis;

import static org.junit.Assert.assertSame;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.redis.core.RedisOperations;

/**
 * @author Costin Leau
 */
public class PropertyEditorsTest {

	private GenericXmlApplicationContext ctx;

	@Before
	public void setUp() {
		ctx = new GenericXmlApplicationContext("/org/springframework/data/redis/pe.xml");
	}

	@After
	public void tearDown() {
		if (ctx != null)
			ctx.destroy();
	}

	@Test
	public void testInjection() throws Exception {
		RedisViewPE bean = ctx.getBean(RedisViewPE.class);
		RedisOperations<?, ?> ops = ctx.getBean(RedisOperations.class);

		assertSame(ops.opsForValue(), bean.getValueOps());
		assertSame(ops.opsForList(), bean.getListOps());
		assertSame(ops.opsForSet(), bean.getSetOps());
		assertSame(ops.opsForZSet(), bean.getZsetOps());
		assertSame(ops, bean.getHashOps().getOperations());
	}
}
