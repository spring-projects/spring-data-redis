/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.connection.rjc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link RjcConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RjcConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@Test
	public void testMultiExec() throws Exception {
		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		connection.multi();
		connection.set(key, value);
		assertNull(connection.get(key));
		List<Object> results = connection.exec();
		assertEquals(2, results.size());
		assertEquals("OK", (String) results.get(0));
		assertEquals(new String(value), new String(RjcUtils.encode((String) results.get(1))));
	}

	@Ignore("nulls are encoded to empty strings")
	public void testNullKey() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testNullValue() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testHashNullKey() throws Exception {
	}

	@Ignore("nulls are encoded to empty strings")
	public void testHashNullValue() throws Exception {
	}

	@Ignore("DATAREDIS-133 Key search does not work with regex")
	public void testKeys() throws Exception {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testDecrByIncrBy() {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testIncDecr() {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testHIncrBy() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSort() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSortStore() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testGetRangeSetRange() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testStrLen() {
	}

	@Ignore("DATAREDIS-120 Pattern matching currently broken")
	public void testPubSubWithPatterns() {
	}

	@Ignore("DATAREDIS-148 Syntax error on RJC zUnionStore")
	public void testZUnionStoreAggWeights() {
	}
}
