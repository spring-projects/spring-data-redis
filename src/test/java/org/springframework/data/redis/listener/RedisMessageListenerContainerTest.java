/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.data.redis.listener;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisMessageListenerContainerTest {

	private JedisConnectionFactory connectionFactory;
	private RedisMessageListenerContainer container;

	private final Object handler = new Object() {
		@SuppressWarnings("unused")
		public void handleMessage(Object message) {
		}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	@Mock private Executor executor;

	@Before
	public void before() throws Exception {

		connectionFactory = new JedisConnectionFactory();
		connectionFactory.setPort(SettingsUtils.getPort());
		connectionFactory.setHostName(SettingsUtils.getHost());
		connectionFactory.setDatabase(2);

		connectionFactory.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(executor);
		container.afterPropertiesSet();
	}

	/*
	 * @see DATAREDIS-415
	 */
	@Test
	public void interruptAtStart() throws Exception {

		final Thread main = Thread.currentThread();

		// interrupt thread once Executor.execute is called
		doAnswer(new Answer() {
			@Override
			public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
				main.interrupt();
				return null;
			}
		}).when(executor).execute(any(Runnable.class));

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.start();

		// reset the interrupted flag to not destroy the teardown
		assertThat(Thread.interrupted(), is(true));

		assertThat(container.isRunning(), is(false));
	}

	@After
	public void tearDown() throws Exception {

		container.destroy();
		connectionFactory.destroy();
	}
}
