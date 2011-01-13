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
package org.springframework.data.keyvalue.redis.listener;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.keyvalue.redis.connection.Message;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;
import org.springframework.util.CollectionUtils;

/**
 * Container providing asynchronous behaviour for Redis message listeners.
 * Handles the low level details of listening, converting and message dispatching.
 * <p/>
 * As oppose to the low level Redis (one connection per subscription), the container 
 * uses only one connection that is 'multiplexed' for all registered listeners, 
 * the message dispatch being done through the task executor.
 * 
 * <p/>
 * Note the container uses the connection only if at least one listener is configured. 
 * 
 * @author Costin Leau
 */
public class RedisListeningContainer implements InitializingBean, DisposableBean, BeanNameAware, SmartLifecycle {

	private static final Log log = LogFactory.getLog(RedisListeningContainer.class);

	private Executor connectionWorker;

	private Executor taskExecutor;

	private RedisConnectionFactory connectionFactory;

	private String beanName;

	private final Object monitor = new Object();
	private volatile boolean running = false;
	private volatile boolean initialized = false;


	// lookup maps
	// to avoid creation of hashes for each message, the maps use raw byte arrays (wrapped to respect the equals/hashcode contract)

	// lookup map between patterns and listeners
	private final Map<ArrayHolder, Collection<MessageListener>> patternMapping = new ConcurrentHashMap<ArrayHolder, Collection<MessageListener>>();
	// lookup map between channels and listeners
	private final Map<ArrayHolder, Collection<MessageListener>> channelMapping = new ConcurrentHashMap<ArrayHolder, Collection<MessageListener>>();

	private final MessageListener multiplexer = new DispatchMessageListener();
	private RedisSerializer<String> serializer = new StringRedisSerializer();


	@Override
	public void afterPropertiesSet() throws Exception {
		//startListening();
		initialized = true;
	}

	@Override
	public void destroy() throws Exception {
		initialized = false;

		// stop listening
		//stopListening();
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPhase() {
		// start the latest
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public void start() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void stop() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the connectionFactory.
	 *
	 * @return Returns the connectionFactory
	 */
	public RedisConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	/**
	 * @param connectionFactory The connectionFactory to set.
	 */
	public void setConnectionFactory(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}


	/**
	 * Sets the serializer for converting the raw channels and patterns into Strings.
	 * By default, {@link StringRedisSerializer} is used.
	 * 
	 * @param serializer The serializer to set.
	 */
	public void setSerializer(RedisSerializer<String> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Attaches the given listeners (and their topics) to the container.
	 * 
	 * <p/>
	 * Note: it's possible to call this method while the container is running forcing a reinitialization
	 * of the container. Note however that this might cause some messages to be lost (while the container
	 * reinitializes) - hence calling this method at runtime is considered advanced usage.
	 * 
	 * @param listeners map of message listeners and their associated topics
	 */
	public void setMessageListeners(Map<? extends MessageListener, Collection<Topic>> listeners) {
		initMapping(listeners);
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running,
	 * the listener starts receiving (matching) messages as soon as possible.
	 * 
	 * @param listener message listener
	 * @param topics message listener topic
	 */
	public void addMessageListener(MessageListener listener, Collection<Topic> topics) {

	}

	private void initMapping(Map<? extends MessageListener, Collection<Topic>> listeners) {
		// stop the listener if currently running
		if (isRunning()) {
			stop();
		}

		patternMapping.clear();
		channelMapping.clear();

		if (!CollectionUtils.isEmpty(listeners)) {
			for (Map.Entry<? extends MessageListener, Collection<Topic>> entry : listeners.entrySet()) {
				addListener(entry.getKey(), entry.getValue());
			}
		}

		// resume activity
		if (initialized) {
			start();
		}
	}

	private void addListener(MessageListener listener, Collection<Topic> topics) {
		for (Topic topic : topics) {

			ArrayHolder holder = new ArrayHolder(serializer.serialize(topic.getTopic()));

			if (topic instanceof ChannelTopic) {
				Collection<MessageListener> collection = channelMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					channelMapping.put(holder, collection);
				}
				collection.add(listener);
			}

			else if (topic instanceof PatternTopic) {
				Collection<MessageListener> collection = patternMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					patternMapping.put(holder, collection);
				}
				collection.add(listener);
			}

			else {
				throw new IllegalArgumentException("Unknown topic type '" + topic.getClass() + "'");
			}
		}
	}

	/**
	 * Actual message dispatcher/multiplexer.
	 * 
	 * @author Costin Leau
	 */
	private class DispatchMessageListener implements MessageListener {

		@Override
		public void onMessage(Message message, byte[] pattern) {
			// do channel matching first
			byte[] channel = message.getChannel();

			Collection<MessageListener> ch = channelMapping.get(new ArrayHolder(channel));
			Collection<MessageListener> pt = null;

			// followed by pattern matching
			if (pattern != null && pattern.length > 0) {
				pt = patternMapping.get(new ArrayHolder(pattern));
			}

			if (!CollectionUtils.isEmpty(ch)) {
				dispatchChannels(ch, message);
			}

			if (!CollectionUtils.isEmpty(pt)) {
				dispatchPatterns(pt, message, pattern);
			}
		}

		private void dispatchChannels(Collection<MessageListener> ch, final Message message) {
			for (final MessageListener messageListener : ch) {
				taskExecutor.execute(new Runnable() {
					@Override
					public void run() {
						messageListener.onMessage(message, null);
					}
				});
			}
		}

		private void dispatchPatterns(Collection<MessageListener> pt, final Message message, final byte[] pattern) {
			for (final MessageListener messageListener : pt) {
				taskExecutor.execute(new Runnable() {
					@Override
					public void run() {
						messageListener.onMessage(message, pattern.clone());
					}
				});
			}
		}
	}

	/**
	 * Simple wrapper class used for wrapping arrays so they can be used as keys inside maps.
	 * 
	 * @author Costin Leau
	 */
	private class ArrayHolder {

		private final byte[] array;
		private final int hashCode;

		ArrayHolder(byte[] array) {
			this.array = array;
			this.hashCode = Arrays.hashCode(array);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ArrayHolder) {
				return Arrays.equals(array, ((ArrayHolder) obj).array);
			}

			return false;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}
}