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
package org.springframework.data.redis.listener.adapter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit test for MessageListenerAdapter.
 * 
 * @author Costin Leau
 */
public class MessageListenerTest {

	private static final RedisSerializer serializer = new StringRedisSerializer();
	private static final String CHANNEL = "some::test:";
	private static final byte[] RAW_CHANNEL = serializer.serialize(CHANNEL);
	private static final String PAYLOAD = "do re mi";
	private static final byte[] RAW_PAYLOAD = serializer.serialize(PAYLOAD);
	private static final Message STRING_MSG = new DefaultMessage(RAW_CHANNEL, RAW_PAYLOAD);

	private MessageListenerAdapter adapter;

	public static interface Delegate {
		void handleMessage(String argument);

		void customMethod(String arg);
	}

	@Mock
	private Delegate target;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		this.adapter = new MessageListenerAdapter();
	}

	@Test
	public void testThatWhenNoDelegateIsSuppliedTheDelegateIsAssumedToBeTheMessageListenerAdapterItself()
			throws Exception {
		assertSame(adapter, adapter.getDelegate());
	}

	@Test
	public void testThatTheDefaultMessageHandlingMethodNameIsTheConstantDefault() throws Exception {
		assertEquals(MessageListenerAdapter.ORIGINAL_DEFAULT_LISTENER_METHOD, adapter.getDefaultListenerMethod());
	}

	@Test
	public void testAdapterWithListenerAndDefaultMessage() throws Exception {
		MessageListener mock = mock(MessageListener.class);

		MessageListenerAdapter adapter = new MessageListenerAdapter(mock);
		adapter.onMessage(STRING_MSG, null);
		verify(mock).onMessage(STRING_MSG, null);
	}


	public void testRawMessage() throws Exception {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target);
		adapter.onMessage(STRING_MSG, null);

		verify(target).handleMessage(PAYLOAD);
	}


	public void testCustomMethod() throws Exception {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target);
		adapter.setDefaultListenerMethod("customMethod");
		adapter.onMessage(STRING_MSG, null);

		verify(target).customMethod(PAYLOAD);
	}
}