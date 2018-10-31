/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.stream;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

/**
 * Simple {@link Executor} based {@link StreamMessageListenerContainer} implementation for running {@link Task tasks} to
 * poll on Redis Streams.
 * <p />
 * This message container creates long-running tasks that are executed on {@link Executor}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class DefaultStreamMessageListenerContainer<K, V> implements StreamMessageListenerContainer<K, V> {

	private final Object lifecycleMonitor = new Object();

	private final Executor taskExecutor;
	private final ErrorHandler errorHandler;
	private final StreamReadOptions readOptions;
	private final RedisTemplate<K, ?> template;

	private final List<Subscription> subscriptions = new ArrayList<>();

	private boolean running = false;

	/**
	 * Create a new {@link DefaultStreamMessageListenerContainer}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param containerOptions must not be {@literal null}.
	 */
	DefaultStreamMessageListenerContainer(RedisConnectionFactory connectionFactory,
			StreamMessageListenerContainerOptions<K, V> containerOptions) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null!");
		Assert.notNull(containerOptions, "StreamMessageListenerContainerOptions must not be null!");

		this.taskExecutor = containerOptions.getExecutor();
		this.errorHandler = containerOptions.getErrorHandler();
		this.readOptions = getStreamReadOptions(containerOptions);
		this.template = createRedisTemplate(connectionFactory, containerOptions);
	}

	private static StreamReadOptions getStreamReadOptions(StreamMessageListenerContainerOptions<?, ?> options) {

		StreamReadOptions streamReadOptions = StreamReadOptions.empty().count(options.getBatchSize());

		if (!options.getPollTimeout().isZero()) {
			streamReadOptions = streamReadOptions.block(options.getPollTimeout());
		}

		return streamReadOptions;
	}

	private RedisTemplate<K, V> createRedisTemplate(RedisConnectionFactory connectionFactory,
			StreamMessageListenerContainerOptions<K, V> containerOptions) {

		RedisTemplate<K, V> template = new RedisTemplate<>();
		template.setKeySerializer(containerOptions.getKeySerializer());
		template.setValueSerializer(containerOptions.getBodySerializer());
		template.setHashKeySerializer(containerOptions.getKeySerializer());
		template.setHashValueSerializer(containerOptions.getBodySerializer());
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		return template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.SmartLifecycle#isAutoStartup()
	 */
	@Override
	public boolean isAutoStartup() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.SmartLifecycle#stop(java.lang.Runnable)
	 */
	@Override
	public void stop(Runnable callback) {

		stop();
		callback.run();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.Lifecycle#start()
	 */
	@Override
	public void start() {

		synchronized (lifecycleMonitor) {

			if (this.running) {
				return;
			}

			subscriptions.stream() //
					.filter(it -> !it.isActive()) //
					.filter(it -> it instanceof TaskSubscription) //
					.map(TaskSubscription.class::cast) //
					.map(TaskSubscription::getTask) //
					.forEach(taskExecutor::execute);

			running = true;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.Lifecycle#stop()
	 */
	@Override
	public void stop() {

		synchronized (lifecycleMonitor) {

			if (this.running) {

				subscriptions.forEach(Cancelable::cancel);

				running = false;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.Lifecycle#isRunning()
	 */
	@Override
	public boolean isRunning() {

		synchronized (this.lifecycleMonitor) {
			return running;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.Phased#getPhase()
	 */
	@Override
	public int getPhase() {
		return Integer.MAX_VALUE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.stream.StreamMessageListenerContainer#register(org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamReadRequest, org.springframework.data.redis.stream.StreamListener)
	 */
	@Override
	public Subscription register(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener) {
		return doRegister(getReadTask(streamRequest, listener));
	}

	private StreamPollTask<K, V> getReadTask(StreamReadRequest<K> streamRequest, StreamListener<K, V> listener) {

		StreamOperations<K, ?, ?> streamOperations = template.opsForStream();

		if (streamRequest instanceof StreamMessageListenerContainer.ConsumerStreamReadRequest) {

			ConsumerStreamReadRequest<K> consumerStreamRequest = (ConsumerStreamReadRequest<K>) streamRequest;

			StreamReadOptions readOptions = consumerStreamRequest.isAutoAck() ? this.readOptions : this.readOptions.noack();
			Consumer consumer = consumerStreamRequest.getConsumer();

			return new StreamPollTask<K, V>(consumerStreamRequest, listener, errorHandler, (key, offset) -> {

				return (List<Record<K, V>>) (List) streamOperations.read(consumer, readOptions,
						StreamOffset.create(key, offset));
				// List<StreamMessage<K, V>> x = (List<StreamMessage<K, V>>)(List) streamOperations.read(consumer, readOptions,
				// StreamOffset.create(key, offset));
				// return x;

			});
		}

		return new StreamPollTask<>(streamRequest, listener, errorHandler, (key, offset) -> {
			return (List<Record<K, V>>) (List) streamOperations.read(readOptions, StreamOffset.create(key, offset));
		});
	}

	private Subscription doRegister(Task task) {

		Subscription subscription = new TaskSubscription(task);

		synchronized (lifecycleMonitor) {

			this.subscriptions.add(subscription);

			if (this.running) {
				taskExecutor.execute(task);
			}
		}

		return subscription;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.MessageListenerContainer#remove(org.springframework.data.mongodb.monitor.Subscription)
	 */
	@Override
	public void remove(Subscription subscription) {

		synchronized (lifecycleMonitor) {

			if (subscriptions.contains(subscription)) {

				if (subscription.isActive()) {
					subscription.cancel();
				}

				subscriptions.remove(subscription);
			}
		}
	}

	/**
	 * {@link Subscription} wrapping a {@link Task}.
	 *
	 * @author Mark Paluch
	 * @since 2.2
	 */
	@EqualsAndHashCode
	@RequiredArgsConstructor
	static class TaskSubscription implements Subscription {

		private final Task task;

		Task getTask() {
			return task;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.stream.Subscription#isActive()
		 */
		@Override
		public boolean isActive() {
			return task.isActive();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.stream.Subscription#await(java.time.Duration)
		 */
		@Override
		public boolean await(Duration timeout) throws InterruptedException {
			return task.awaitStart(timeout);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.stream.Cancelable#cancel()
		 */
		@Override
		public void cancel() throws DataAccessResourceFailureException {
			task.cancel();
		}
	}

	/**
	 * Logging {@link ErrorHandler}.
	 *
	 * @author Mark Paluch
	 * @since 2.2
	 */
	enum LoggingErrorHandler implements ErrorHandler {

		INSTANCE;

		private final Log logger;

		LoggingErrorHandler() {
			this.logger = LogFactory.getLog(LoggingErrorHandler.class);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.util.ErrorHandler#handleError(java.lang.Throwable)
		 */
		public void handleError(Throwable t) {

			if (this.logger.isErrorEnabled()) {
				this.logger.error("Unexpected error occurred in scheduled task.", t);
			}
		}
	}
}
