/*
 * Copyright 2015-2019 the original author or authors.
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

import java.util.List;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base {@link MessageListener} implementation for listening to Redis keyspace notifications.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public abstract class KeyspaceEventMessageListener implements MessageListener, InitializingBean, DisposableBean {

	private final RedisMessageListenerContainer listenerContainer;
	private static final Topic TOPIC_ALL_KEYEVENTS = new PatternTopic("__keyevent@*");
	private String keyspaceNotificationsConfigParameter = "EA";

	/**
	 * Creates new {@link KeyspaceEventMessageListener}.
	 * 
	 * @param listenerContainer must not be {@literal null}.
	 */
	public KeyspaceEventMessageListener(RedisMessageListenerContainer listenerContainer) {

		Assert.notNull(listenerContainer, "RedisMessageListenerContainer to run in must not be null!");
		this.listenerContainer = listenerContainer;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.MessageListener#onMessage(org.springframework.data.redis.connection.Message, byte[])
	 */
	@Override
	public void onMessage(Message message, byte[] pattern) {

		if (message == null || message.getChannel() == null || message.getBody() == null) {
			return;
		}

		doHandleMessage(message);
	}

	/**
	 * Handle the actual message
	 * 
	 * @param message never {@literal null}.
	 */
	protected abstract void doHandleMessage(Message message);

	/**
	 * Initialize the message listener by writing requried redis config for {@literal notify-keyspace-events} and
	 * registering the listener within the container.
	 */
	public void init() {

		if (StringUtils.hasText(keyspaceNotificationsConfigParameter)) {

			RedisConnection connection = listenerContainer.getConnectionFactory().getConnection();

			try {

				List<String> config = connection.getConfig("notify-keyspace-events");

				if (config.size() == 2 && !StringUtils.hasText(config.get(1))) {
					connection.setConfig("notify-keyspace-events", keyspaceNotificationsConfigParameter);
				}

			} finally {
				connection.close();
			}
		}

		doRegister(listenerContainer);
	}

	/**
	 * Register instance within the container.
	 * 
	 * @param container never {@literal null}.
	 */
	protected void doRegister(RedisMessageListenerContainer container) {
		listenerContainer.addMessageListener(this, TOPIC_ALL_KEYEVENTS);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		listenerContainer.removeMessageListener(this);
	}

	/**
	 * Set the configuration string to use for {@literal notify-keyspace-events}.
	 *
	 * @param keyspaceNotificationsConfigParameter can be {@literal null}.
	 * @since 1.8
	 */
	public void setKeyspaceNotificationsConfigParameter(String keyspaceNotificationsConfigParameter) {
		this.keyspaceNotificationsConfigParameter = keyspaceNotificationsConfigParameter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		init();
	}
}
