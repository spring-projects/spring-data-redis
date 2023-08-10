/*
 * Copyright 2015-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.listener;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Base {@link MessageListener} implementation for listening to Redis keyspace notifications.
 * <p>
 * By default, this {@link MessageListener} does not listen for, or notify on, any keyspace events. You must explicitly
 * set the {@link #setKeyspaceNotificationsConfigParameter(String)} to a valid {@literal redis.conf},
 * {@literal notify-keyspace-events} value (for example: {@literal EA}) to enable keyspace event notifications
 * from your Redis server.
 * <p>
 * Any configuration set in the Redis server take precedence. Therefore, if the Redis server already set a value
 * for {@literal notify-keyspace-events}, then any {@link #setKeyspaceNotificationsConfigParameter(String)}
 * specified on this listener will be ignored.
 * <p>
 * It is recommended that all infrastructure settings, such as {@literal notify-keyspace-events}, be configured on
 * the Redis server itself. If the Redis server is rebooted, then any keyspace event configuration coming from
 * the application will be lost when the Redis server is restarted since Redis server configuration is not persistent,
 * and any configuration coming from your application only occurs during Spring container initialization.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @since 1.7
 */
public abstract class KeyspaceEventMessageListener implements MessageListener, InitializingBean, DisposableBean {

	protected static final String DISABLED_KEY_EVENTS = "";
	protected static final String NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

	private static final Topic TOPIC_ALL_KEYEVENTS = new PatternTopic("__keyevent@*");

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final RedisMessageListenerContainer messageListenerContainer;

	private @Nullable String keyspaceNotificationsConfigParameter;

	/**
	 * Creates a new {@link KeyspaceEventMessageListener}.
	 *
	 * @param messageListenerContainer {@link RedisMessageListenerContainer} in which this listener will be registered;
	 * must not be {@literal null}.
	 */
	public KeyspaceEventMessageListener(RedisMessageListenerContainer messageListenerContainer) {
		this(messageListenerContainer, DISABLED_KEY_EVENTS);
	}

	/**
	 * Creates a new {@link KeyspaceEventMessageListener} along with initialization for
	 * {@literal notify-keyspace-events}.
	 *
	 * @param messageListenerContainer {@link RedisMessageListenerContainer} in which this listener will be registered;
	 * must not be {@literal null}.
	 * @param keyspaceNotificationsConfigParameter {@link String default value} for {@literal notify-keyspace-events};
	 * may be {@literal null}.
	 */
	protected KeyspaceEventMessageListener(RedisMessageListenerContainer messageListenerContainer,
			@Nullable String keyspaceNotificationsConfigParameter) {

		Assert.notNull(messageListenerContainer, "RedisMessageListenerContainer to run in must not be null");

		this.messageListenerContainer = messageListenerContainer;
		this.keyspaceNotificationsConfigParameter = keyspaceNotificationsConfigParameter;
	}

	/**
	 * Returns a reference to the configured {@link Logger}.
	 *
	 * @return a reference to the configured {@link Logger}.
	 */
	protected Logger getLogger() {
		return this.logger;
	}

	/**
	 * Returns a configured reference to the {@link RedisMessageListenerContainer} to which this {@link MessageListener}
	 * is registered.
	 *
	 * @return a configured reference to the {@link RedisMessageListenerContainer} to which this {@link MessageListener}
	 * is registered.
	 */
	protected RedisMessageListenerContainer getMessageListenerContainer() {
		return this.messageListenerContainer;
	}

	@Override
	public void onMessage(Message message, @Nullable byte[] pattern) {

		if (containsChannelContent(message)) {
			doHandleMessage(message);
		}
	}

	// Message must have a channel and body (contain content)
	private boolean containsChannelContent(Message message) {
		return !(ObjectUtils.isEmpty(message.getChannel()) || ObjectUtils.isEmpty(message.getBody()));
	}

	/**
	 * Handle the actual {@link Message}.
	 *
	 * @param message {@link Message} to process; never {@literal null}.
	 */
	protected abstract void doHandleMessage(Message message);

	@Override
	public void afterPropertiesSet() throws Exception {
		init();
	}

	/**
	 * Initialize this {@link MessageListener} by writing required Redis server config
	 * for {@literal notify-keyspace-events} and registering this {@link MessageListener}
	 * with the {@link RedisMessageListenerContainer}.
	 */
	public void init() {

		String keyspaceNotificationsConfigParameter = getKeyspaceNotificationsConfigParameter();

		if (isSet(keyspaceNotificationsConfigParameter)) {
			configureKeyspaceEventNotifications(keyspaceNotificationsConfigParameter);
		}

		doRegister(getMessageListenerContainer());
	}

	private boolean isSet(@Nullable String value) {
		return StringUtils.hasText(value);
	}

	void configureKeyspaceEventNotifications(String keyspaceNotificationsConfigParameter) {

		RedisConnectionFactory connectionFactory = getMessageListenerContainer().getConnectionFactory();

		if (connectionFactory != null) {
			try (RedisConnection connection = connectionFactory.getConnection()) {
				if (canChangeNotifyKeyspaceEvents(connection)) {
					setKeyspaceEventNotifications(connection, keyspaceNotificationsConfigParameter);
				}
			}
		}
		else {
			if (getLogger().isWarnEnabled()) {
				getLogger().warn("Unable to configure notification on keyspace events;"
					+ " no RedisConnectionFactory was configured in the RedisMessageListenerContainer");
			}
		}
	}

	private boolean canChangeNotifyKeyspaceEvents(@Nullable RedisConnection connection) {

		if (connection != null) {

			Properties config = connection.serverCommands().getConfig(NOTIFY_KEYSPACE_EVENTS);

			return config == null || !isSet(config.getProperty(NOTIFY_KEYSPACE_EVENTS));
		}

		return false;
	}

	void setKeyspaceEventNotifications(RedisConnection connection, String keyspaceNotificationsConfigParameter) {
		connection.serverCommands().setConfig(NOTIFY_KEYSPACE_EVENTS, keyspaceNotificationsConfigParameter);
	}

	@Override
	public void destroy() throws Exception {
		getMessageListenerContainer().removeMessageListener(this);
	}

	/**
	 * Register instance within the {@link RedisMessageListenerContainer}.
	 *
	 * @param container never {@literal null}.
	 */
	protected void doRegister(RedisMessageListenerContainer container) {
		container.addMessageListener(this, TOPIC_ALL_KEYEVENTS);
	}

	/**
	 * Set the {@link String configuration setting} (for example: {@literal EA}) to use
	 * for {@literal notify-keyspace-events}.
	 *
	 * @param keyspaceNotificationsConfigParameter can be {@literal null}.
	 * @since 1.8
	 */
	public void setKeyspaceNotificationsConfigParameter(String keyspaceNotificationsConfigParameter) {
		this.keyspaceNotificationsConfigParameter = keyspaceNotificationsConfigParameter;
	}

	/**
	 * Get the configured {@link String setting} for {@literal notify-keyspace-events}.
	 *
	 * @return the configured {@link String setting} for {@literal notify-keyspace-events}.
	 */
	@Nullable
	protected String getKeyspaceNotificationsConfigParameter() {
		return this.keyspaceNotificationsConfigParameter;
	}
}
