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
package org.springframework.data.redis.connection.srp;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link SrpConnection} pipeline functionality
 * 
 * @author Jennifer Hickey
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("SrpConnectionIntegrationTests-context.xml")
public class SrpConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayOKs() {
		// SRP returns the Strings from individual StatusReplys in a
		// MultiBulkReply, while other clients return as byte[]
		actual.add(connection.eval("return { redis.call('set','abc','ghk'),  redis.call('set','abc','lfdf')}",
				ReturnType.MULTI, 0));
		verifyResults(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "OK", "OK" }) }));
	}

	@Test(expected = RedisSystemException.class)
	public void testExecWithoutMulti() {
		connection.exec();
		// SRP throws an Exception right away on exec instead of once pipeline is closed
		getResults();
	}

	@Test
	public void testExecuteNoArgs() {
		// SRP returns this as String while other drivers return as byte[]
		actual.add(connection.execute("PING"));
		verifyResults(Arrays.asList(new Object[] { "PONG" }));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}

	@SuppressWarnings("unchecked")
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayBytes() {
		getResults();
		byte[] sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}").getBytes();
		initConnection();
		actual.add(byteConnection.evalSha(sha1, ReturnType.MULTI, 1, "key1".getBytes(), "arg1".getBytes()));
		List<Object> results = getResults();
		List<byte[]> scriptResults = (List<byte[]>) results.get(0);
		assertEquals(Arrays.asList(new Object[] { "key1", "arg1" }),
				Arrays.asList(new Object[] { new String(scriptResults.get(0)), new String(scriptResults.get(1)) }));
	}

}
