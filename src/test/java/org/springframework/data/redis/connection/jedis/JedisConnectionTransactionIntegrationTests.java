package org.springframework.data.redis.connection.jedis;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionTransactionIntegrationTests extends
		JedisConnectionPipelineIntegrationTests {

	@Ignore
	public void testMultiDiscard() {
	}

	@Ignore
	public void testMultiExec() {
	}

	@Ignore
	public void testUnwatch() {
	}

	@Ignore
	public void testWatch() {
	}

	// Unsupported Ops

	@Test(expected = RedisSystemException.class)
	public void testGetConfig() {
		connection.getConfig("*");
	}

	@Test(expected = RedisSystemException.class)
	public void testEcho() {
		super.testEcho();
	}

	@Test
	public void exceptionExecuteNative() throws Exception {
		actual.add(connection.execute("ZadD", getClass() + "#foo\t0.90\titem"));
		// Syntax error on queued commands are swallowed and no results are
		// returned
		verifyResults(Arrays.asList(new Object[] {}), actual);
	}

	protected void initConnection() {
		connection.multi();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
