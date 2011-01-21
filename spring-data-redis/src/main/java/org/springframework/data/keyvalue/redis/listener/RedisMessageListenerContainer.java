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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.keyvalue.redis.connection.Message;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.Subscription;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.ClassUtils;
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
 * Note the container uses the connection in a lazy fashion (the connection is used only if at least one listener is configured). 
 * 
 * @author Costin Leau
 */
public class RedisMessageListenerContainer implements InitializingBean, DisposableBean, BeanNameAware, SmartLifecycle {

	private static final Log log = LogFactory.getLog(RedisMessageListenerContainer.class);

	/**
	 * Default thread name prefix: "RedisListeningContainer-".
	 */
	public static final String DEFAULT_THREAD_NAME_PREFIX = ClassUtils.getShortName(RedisMessageListenerContainer.class)
			+ "-";


	private Executor subscriptionExecutor;

	private Executor taskExecutor;

	private RedisConnectionFactory connectionFactory;

	private String beanName;


	private final Object monitor = new Object();
	// whether the container is running (or not)
	private volatile boolean running = false;
	// whether the container has been initialized
	private volatile boolean initialized = false;
	// whether the container uses a connection or not
	// (as the container might be running but w/o listeners, it won't use any resources)
	private volatile boolean listening = false;

	private volatile boolean manageExecutor = false;


	// lookup maps
	// to avoid creation of hashes for each message, the maps use raw byte arrays (wrapped to respect the equals/hashcode contract)

	// lookup map between patterns and listeners
	private final Map<ArrayHolder, Collection<MessageListener>> patternMapping = new ConcurrentHashMap<ArrayHolder, Collection<MessageListener>>();
	// lookup map between channels and listeners
	private final Map<ArrayHolder, Collection<MessageListener>> channelMapping = new ConcurrentHashMap<ArrayHolder, Collection<MessageListener>>();

	private final SubscriptionTask subscriptionTask = new SubscriptionTask();

	private final MessageListener multiplexer = new DispatchMessageListener();
	private volatile RedisSerializer<String> serializer = new StringRedisSerializer();


	@Override
	public void afterPropertiesSet() {
		if (taskExecutor == null) {
			manageExecutor = true;
			taskExecutor = createDefaultTaskExecutor();
		}

		if (subscriptionExecutor == null) {
			subscriptionExecutor = taskExecutor;
		}

		start();
		initialized = true;
	}

	/**
	 * Creates a default TaskExecutor. Called if no explicit TaskExecutor has been specified.
	 * <p>The default implementation builds a {@link org.springframework.core.task.SimpleAsyncTaskExecutor}
	 * with the specified bean name (or the class name, if no bean name specified) as thread name prefix.
	 * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
	 */
	protected TaskExecutor createDefaultTaskExecutor() {
		String threadNamePrefix = (beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
		return new SimpleAsyncTaskExecutor(threadNamePrefix);
	}

	@Override
	public void destroy() throws Exception {
		initialized = false;

		stop();

		if (manageExecutor) {
			if (taskExecutor instanceof DisposableBean) {
				((DisposableBean) taskExecutor).destroy();

				if (log.isDebugEnabled()) {
					log.debug("Stopped internally-managed task executor");
				}
			}
		}
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
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
		if (!running) {
			running = true;
			lazyListen();
			if (log.isDebugEnabled()) {
				log.debug("Started RedisMessageListenerContainer");
			}
		}
	}

	@Override
	public void stop() {
		running = false;
		subscriptionTask.cancel();

		if (log.isDebugEnabled()) {
			log.debug("Stopped RedisMessageListenerContainer");
		}
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
	 * Sets the task executor used for running the message listeners when messages are received.
	 * If no task executor is set, an instance of {@link SimpleAsyncTaskExecutor} will be used by default.
	 * The task executor can be adjusted depending on the work done by the listeners and the number of 
	 * messages coming in.
	 * 
	 * @param taskExecutor The taskExecutor to set.
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Sets the task execution used for subscribing to Redis channels. By default, if no executor is set,
	 * the {@link #setTaskExecutor(Executor)} will be used. In some cases, this might be undersired as
	 * the listening to the connection is a long running task.
	 *
	 * <p/>Note: This implementation uses at most one long running thread (depending on whether there are any listeners registered or not)
	 * and up to two threads during the initial registration. 
	 * 
	 * @param subscriptionExecutor The subscriptionExecutor to set.
	 */
	public void setSubscriptionExecutor(Executor subscriptionExecutor) {
		this.subscriptionExecutor = subscriptionExecutor;
	}

	/**
	 * Sets the serializer for converting the {@link Topic}s into low-level channels and patterns.
	 * By default, {@link StringRedisSerializer} is used.
	 * 
	 * @param serializer The serializer to set.
	 */
	public void setTopicSerializer(RedisSerializer<String> serializer) {
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
	public void setMessageListeners(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {
		initMapping(listeners);
	}

	/**
	 * Adds a message listener to the (potentially running) container. If the container is running,
	 * the listener starts receiving (matching) messages as soon as possible.
	 * 
	 * @param listener message listener
	 * @param topics message listener topic
	 */
	public void addMessageListener(MessageListener listener, Collection<? extends Topic> topics) {
		addListener(listener, topics);
		lazyListen();
	}

	private void initMapping(Map<? extends MessageListener, Collection<? extends Topic>> listeners) {
		// stop the listener if currently running
		if (isRunning()) {
			stop();
		}

		patternMapping.clear();
		channelMapping.clear();

		if (!CollectionUtils.isEmpty(listeners)) {
			for (Map.Entry<? extends MessageListener, Collection<? extends Topic>> entry : listeners.entrySet()) {
				addListener(entry.getKey(), entry.getValue());
			}
		}

		// resume activity
		if (initialized) {
			start();
		}
	}

	/**
	 * Method inspecting whether listening for messages (and thus using a thread) is actually needed and triggering it.
	 */
	private void lazyListen() {
		boolean debug = log.isDebugEnabled();
		boolean started = false;

		if (isRunning()) {
			if (!listening) {
				synchronized (monitor) {
					if (!listening) {
						if (channelMapping.size() > 0 || patternMapping.size() > 0) {
							subscriptionExecutor.execute(subscriptionTask);
							listening = true;
							started = true;
						}
					}
					else {
						listening = false;
					}

				}
				if (debug) {
					if (started) {
						log.debug("Started listening for Redis messages");
					}
					else {
						log.debug("Postpone listening for Redis messages until actual listeners are added");
					}
				}
			}
		}
	}

	private void addListener(MessageListener listener, Collection<? extends Topic> topics) {
		List<byte[]> channels = new ArrayList<byte[]>(topics.size());
		List<byte[]> patterns = new ArrayList<byte[]>(topics.size());

		boolean trace = log.isTraceEnabled();

		for (Topic topic : topics) {

			ArrayHolder holder = new ArrayHolder(serializer.serialize(topic.getTopic()));

			if (topic instanceof ChannelTopic) {
				Collection<MessageListener> collection = channelMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					channelMapping.put(holder, collection);
				}
				collection.add(listener);
				channels.add(holder.array);

				if (trace)
					log.trace("Adding listener '" + listener + "' on channel '" + topic.getTopic() + "'");
			}

			else if (topic instanceof PatternTopic) {
				Collection<MessageListener> collection = patternMapping.get(holder);
				if (collection == null) {
					collection = new CopyOnWriteArraySet<MessageListener>();
					patternMapping.put(holder, collection);
				}
				collection.add(listener);
				patterns.add(holder.array);

				if (trace)
					log.trace("Adding listener '" + listener + "' for pattern '" + topic.getTopic() + "'");
			}

			else {
				throw new IllegalArgumentException("Unknown topic type '" + topic.getClass() + "'");
			}
		}

		// check the current listening state
		if (listening) {
			subscriptionTask.subscribeChannel(channels.toArray(new byte[channels.size()][]));
			subscriptionTask.subscribePattern(patterns.toArray(new byte[patterns.size()][]));
		}
	}

	/**
	 * Runnable used for Redis subscription. Implemented as a dedicated class to provide as many hints
	 * as possible to the underlying thread pool.
	 * 
	 * @author Costin Leau
	 */
	private class SubscriptionTask implements SchedulingAwareRunnable {

		/**
		 * Runnable used, on a parallel thread, to do the initial pSubscribe.
		 * This is required since, during initialization, both subscribe and pSubscribe
		 * might be needed but since the first call is blocking, the second call needs to
		 * executed in parallel.
		 *  
		 * @author Costin Leau
		 */
		private class PatternSubscriptionTask implements SchedulingAwareRunnable {

			private long WAIT = 1000;
			private long ROUNDS = 3;

			@Override
			public boolean isLongLived() {
				return false;
			}

			@Override
			public void run() {
				// wait for subscription to be initialized
				boolean done = false;
				// wait 3 rounds for subscription to be initialized
				for (int i = 0; i < ROUNDS || done; i++) {
					if (connection != null) {
						synchronized (localMonitor) {
							if (connection != null && connection.isSubscribed()) {
								done = true;
								connection.getSubscription().pSubscribe(unwrap(patternMapping.keySet()));
							}
							else {
								try {
									Thread.sleep(WAIT);
								} catch (InterruptedException ex) {
									done = true;
								}
							}
						}
					}
				}
			}
		}

		private volatile RedisConnection connection;
		private final Object localMonitor = new Object();

		@Override
		public boolean isLongLived() {
			return true;
		}

		@Override
		public void run() {
			connection = connectionFactory.getConnection();
			try {
				if (connection.isSubscribed()) {
					throw new IllegalStateException("Retrieved connection is already subscribed; aborting listening");
				}

				// NB: each Xsubscribe call blocks

				// subscribe one way or the other
				// and schedule the rest
				if (!channelMapping.isEmpty()) {
					// schedule the rest of the subscription
					if (!patternMapping.isEmpty()) {
						subscriptionExecutor.execute(new PatternSubscriptionTask());
					}
					connection.subscribe(new DispatchMessageListener(), unwrap(channelMapping.keySet()));
				}
				else {
					connection.pSubscribe(new DispatchMessageListener(), unwrap(patternMapping.keySet()));
				}

			} finally {
				// this block is executed once the subscription has ended
				// meaning cleanup is required
				listening = false;

				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							connection.close();
							connection = null;
						}
					}
				}
			}
		}

		private byte[][] unwrap(Collection<ArrayHolder> holders) {
			if (CollectionUtils.isEmpty(holders)) {
				return new byte[0][];
			}

			byte[][] unwrapped = new byte[holders.size()][];

			int index = 0;
			for (ArrayHolder arrayHolder : holders) {
				unwrapped[index++] = arrayHolder.array;
			}

			return unwrapped;
		}

		void cancel() {
			if (connection != null) {
				synchronized (localMonitor) {
					if (connection != null) {
						Subscription sub = connection.getSubscription();
						if (sub != null) {
							sub.pUnsubscribe();
							sub.unsubscribe();
						}
					}
				}
			}
		}

		void subscribeChannel(byte[]... channels) {
			if (channels != null && channels.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.subscribe(channels);
							}
						}
					}
				}
			}
		}

		void subscribePattern(byte[]... patterns) {
			if (patterns != null && patterns.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.pSubscribe(patterns);
							}
						}
					}
				}
			}
		}

		void unsubscribeChannel(byte[]... channels) {
			if (channels != null && channels.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.unsubscribe(channels);
							}
						}
					}
				}
			}
		}

		void unsubscribePattern(byte[]... patterns) {
			if (patterns != null && patterns.length > 0) {
				if (connection != null) {
					synchronized (localMonitor) {
						if (connection != null) {
							Subscription sub = connection.getSubscription();
							if (sub != null) {
								sub.pUnsubscribe(patterns);
							}
						}
					}
				}
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