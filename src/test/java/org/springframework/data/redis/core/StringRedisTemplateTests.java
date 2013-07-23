/*
 * Copyright 2013 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * Integration test of {@link StringRedisTemplate}
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@ProfileValueSourceConfiguration(RedisTestProfileValueSource.class)
public class StringRedisTemplateTests {

	@Autowired
	private StringRedisTemplate redisTemplate;

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testDumpAndRestoreNoTtl() {
		redisTemplate.boundValueOps("testing").set("123");
		byte[] serializedValue = redisTemplate.dump("testing");
		assertNotNull(serializedValue);
		redisTemplate.delete("testing");
		redisTemplate.restore("testing", serializedValue, 0, TimeUnit.SECONDS);
		assertEquals("123", redisTemplate.boundValueOps("testing").get());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		redisTemplate.boundValueOps("testing").set("123");
		byte[] serializedValue = redisTemplate.dump("testing");
		assertNotNull(serializedValue);
		redisTemplate.delete("testing");
		redisTemplate.restore("testing", serializedValue, 200, TimeUnit.MILLISECONDS);
		assertEquals("123", redisTemplate.boundValueOps("testing").get());
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey("testing"));
			}
		}, 400);
	}

	@Test
	public void testKeys() throws Exception {
		redisTemplate.opsForValue().set("foo", "bar");
		assertNotNull(redisTemplate.keys("*"));
	}

	@SuppressWarnings("rawtypes")
	@Test(expected = IllegalArgumentException.class)
	public void testTemplateNotInitialized() throws Exception {
		RedisTemplate tpl = new RedisTemplate();
		tpl.setConnectionFactory(redisTemplate.getConnectionFactory());
		tpl.exec();
	}

	@Test
	public void testStringTemplateExecutesWithStringConn() {
		String value = redisTemplate.execute(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) {
				StringRedisConnection stringConn = (StringRedisConnection) connection;
				stringConn.set("test", "it");
				return stringConn.get("test");
			}
		});
		assertEquals(value,"it");
	}

	@Test
	public void testStringTemplateExecutePipelineResultsConverted() {
		String result = redisTemplate.execute(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) {
				StringRedisConnection stringConn = (StringRedisConnection) connection;
				stringConn.openPipeline();
				stringConn.set("foo", "bar");
				stringConn.get("foo");
				List<Object> results = stringConn.closePipeline();
				return (String) results.get(0);
			}
		});
		assertEquals("bar",result);
	}

	@Test
	public void testStringTemplateExecutePipelineResultsNotConverted() {
		final StringRedisTemplate template2 = new StringRedisTemplate(redisTemplate.getConnectionFactory());
		template2.setDeserializePipelineAndTxResults(false);
		template2.afterPropertiesSet();
		String result = template2.execute(new RedisCallback<String>() {
			public String doInRedis(RedisConnection connection) {
				StringRedisConnection stringConn = (StringRedisConnection) connection;
				stringConn.openPipeline();
				stringConn.set("foo", "bar");
				stringConn.get("foo");
				List<Object> results = stringConn.closePipeline();
				// Results should be in byte[], not deserialized to String
				return template2.getStringSerializer().deserialize((byte[]) results.get(0));
			}
		});
		assertEquals("bar",result);
	}
}
