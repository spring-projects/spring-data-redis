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

package org.springframework.data.redis.connection.jredis;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JRedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {
	
	@After
	public void tearDown() {
		try {
			connection.close();
		}catch(DataAccessException e) {
			// Jredis closes a connection on Exception (which some tests intentionally throw)
			// Attempting to close the connection again will result in error
			System.out.println("Connection already closed");
		}
		connection = null;
	}

	@Ignore("JRedis does not support pipelining")
	public void testNullCollections() {
	}

	@Ignore
	public void testNullKey() throws Exception {
	}

	@Ignore
	public void testNullValue() throws Exception {
	}

	@Ignore
	public void testHashNullKey() throws Exception {
	}

	@Ignore
	public void testHashNullValue() throws Exception {
	}

	@Ignore
	public void testNullSerialization() throws Exception {
	}

	@Ignore
	public void testPubSub() throws Exception {
	}

	@Ignore
	public void testPubSubWithPatterns() {
	}

	@Ignore
	public void testPubSubWithNamedChannels() {
	}

	@Ignore
	public void testBitSet() throws Exception {
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testMultiExec() throws Exception {
		super.testMultiExec();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testMultiDiscard() throws Exception {
		super.testMultiDiscard();
	}

	@Ignore
	public void exceptionExecuteNativeWithPipeline() throws Exception {
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testExceptionExecuteNativeWithPipeline() throws Exception {
		super.testExceptionExecuteNativeWithPipeline();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testExecuteNativeWithPipeline() throws Exception {
		super.testExecuteNativeWithPipeline();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBlPopTimeout() {
		super.testBlPopTimeout();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBlPop() {
		super.testBlPop();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPop() {
		super.testBRPop();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testBRPopTimeout() {
		super.testBRPopTimeout();
	}
}