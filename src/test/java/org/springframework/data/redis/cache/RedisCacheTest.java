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

package org.springframework.data.redis.cache;

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.cache.Cache;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.CollectionTestParams;
import org.springframework.data.redis.support.collections.ObjectFactory;

/**
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class RedisCacheTest extends AbstractNativeCacheTest<RedisTemplate> {

	private ObjectFactory<Object> objFactory;
	private RedisTemplate template;


	public RedisCacheTest(ObjectFactory<Object> objFactory, RedisTemplate template) {
		this.objFactory = objFactory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return CollectionTestParams.testParams();
	}

	@Override
	protected Cache createCache(RedisTemplate nativeCache) {
		return new RedisCache(CACHE_NAME, nativeCache);
	}

	@Override
	protected RedisTemplate createNativeCache() throws Exception {
		return template;
	}

	@Before
	public void setUp() throws Exception {
		ConnectionFactoryTracker.add(template.getConnectionFactory());
		super.setUp();

	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Override
	protected Object getObject() {
		return objFactory.instance();
	}
}