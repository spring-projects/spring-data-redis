/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.StringMessage;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * Unit tests for {@link BatchMethodRedisListenerEndpoint}.
 *
 * @author Moritz Halbritter
 */
class BatchMethodRedisListenerEndpointUnitTests {

	private static final String TOPIC = "ch1";

	private final MessageHandlerMethodFactory factory = new RedisListenerEndpointRegistrar().getMessageHandlerMethodFactory();

	@Test // GH-3439
	void shouldRejectEmptyEndpoints() {
		assertThatIllegalArgumentException().isThrownBy(() -> new BatchMethodRedisListenerEndpoint(List.of()));
	}

	@Test // GH-3439
	void shouldRejectDifferentBeans() throws NoSuchMethodException {

		MethodRedisListenerEndpoint one = endpointFor(new TestBean(), "a", "ch1");
		MethodRedisListenerEndpoint two = endpointFor(new TestBean(), "b", "ch1");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BatchMethodRedisListenerEndpoint(List.of(one, two)));
	}

	@Test // GH-3439
	void shouldRejectMissingTopic() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		MethodRedisListenerEndpoint one = endpointFor(bean, "a", TOPIC);
		MethodRedisListenerEndpoint two = endpointFor(bean, "b", null);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new BatchMethodRedisListenerEndpoint(List.of(one, two)));
	}

	@Test // GH-3439
	void shouldSubscribeAllTopicsOnce() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		BatchMethodRedisListenerEndpoint batch = new BatchMethodRedisListenerEndpoint(List.of(endpointFor(bean, "a", "ch1"),
				endpointFor(bean, "b", "ch1"), endpointFor(bean, "c", "news.*")));
		RedisMessageListenerContainer container = container();

		batch.register(container);
		batch.start();

		verify(container).addMessageListener(any(MessageListener.class),
				eq(Set.of(ChannelTopic.of("ch1"), PatternTopic.of("news.*"))));
	}

	@Test // GH-3439
	void shouldRouteByChannel() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		MessageListener listener = registerAndStart(new BatchMethodRedisListenerEndpoint(
				List.of(endpointFor(bean, "a", "ch1"), endpointFor(bean, "b", "ch2"))), container());

		listener.onMessage(new StringMessage("ch2", "hi"), null);
		listener.onMessage(new StringMessage("ch3", "ignored"), null);

		assertThat(bean.invocations).containsExactly("b:hi");
	}

	@Test // GH-3439
	void shouldRouteByPattern() throws NoSuchMethodException {

		// a message on news.1 is received twice: once for the channel, once for the matching pattern
		TestBean bean = new TestBean();
		MessageListener listener = registerAndStart(new BatchMethodRedisListenerEndpoint(
				List.of(endpointFor(bean, "a", "news.1"), endpointFor(bean, "b", "news.*"))), container());

		listener.onMessage(new StringMessage("news.1", "channel"), null);
		listener.onMessage(new StringMessage("news.1", "pattern"), "news.*".getBytes());

		assertThat(bean.invocations).containsExactly("a:channel", "b:pattern");
	}

	@Test // GH-3439
	void shouldRouteWithContainerTopicSerializer() throws NoSuchMethodException {

		// container prefixes every topic, e.g. ch1 is subscribed as app:ch1
		RedisMessageListenerContainer container = mock(RedisMessageListenerContainer.class);
		when(container.getTopicSerializer()).thenReturn(new PrefixingSerializer("app:"));

		TestBean bean = new TestBean();
		MessageListener listener = registerAndStart(batchFor(bean), container);

		listener.onMessage(new StringMessage(TOPIC, "unprefixed"), null);
		listener.onMessage(new StringMessage("app:" + TOPIC, "hi"), null);

		assertThat(bean.invocations).containsExactlyInAnyOrder("a:hi", "b:hi");
	}

	@Test // GH-3439
	void shouldInvokeEveryMethodOnce() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		MessageListener listener = registerAndStart(batchFor(bean));

		listener.onMessage(new StringMessage(TOPIC, "hi"), null);

		assertThat(bean.invocations).containsExactlyInAnyOrder("a:hi", "b:hi");
	}

	@Test // GH-3439
	void shouldForwardSubscriptionCallbacksOnce() throws NoSuchMethodException {

		SubscriptionAwareBean bean = mock(SubscriptionAwareBean.class);
		MessageListener listener = registerAndStart(batchFor(bean));

		assertThat(listener).isInstanceOf(SubscriptionListener.class);

		SubscriptionListener subscriptionListener = (SubscriptionListener) listener;
		subscriptionListener.onChannelSubscribed("ch1".getBytes(), 1);
		subscriptionListener.onChannelUnsubscribed("ch1".getBytes(), 0);
		subscriptionListener.onPatternSubscribed("ch*".getBytes(), 1);
		subscriptionListener.onPatternUnsubscribed("ch*".getBytes(), 0);

		verify(bean, times(1)).onChannelSubscribed("ch1".getBytes(), 1);
		verify(bean, times(1)).onChannelUnsubscribed("ch1".getBytes(), 0);
		verify(bean, times(1)).onPatternSubscribed("ch*".getBytes(), 1);
		verify(bean, times(1)).onPatternUnsubscribed("ch*".getBytes(), 0);
	}

	@Test // GH-3439
	void shouldInvokeRemainingListenersAndRethrowWhenOneThrows() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		MethodRedisListenerEndpoint first = customEndpointFor(bean, "a", handler -> new ThrowingAdapter(handler, "first"));
		MethodRedisListenerEndpoint ok = endpointFor(bean, "b", TOPIC);
		MethodRedisListenerEndpoint second = customEndpointFor(bean, "a", handler -> new ThrowingAdapter(handler, "second"));

		MessageListener listener = registerAndStart(new BatchMethodRedisListenerEndpoint(List.of(first, ok, second)));

		assertThatIllegalStateException().isThrownBy(() -> listener.onMessage(new StringMessage(TOPIC, "hi"), null))
				.withMessage("first").satisfies(ex -> assertThat(ex.getSuppressed()).singleElement()
						.satisfies(suppressed -> assertThat(suppressed).hasMessage("second")));

		assertThat(bean.invocations).containsExactly("b:hi");
	}

	@Test // GH-3439
	void shouldForwardSubscriptionCallbacksThroughChildListener() throws NoSuchMethodException {

		TestBean bean = new TestBean();
		SubscriptionListener custom = mock(SubscriptionListener.class);
		MethodRedisListenerEndpoint one = customEndpointFor(bean, "a", handler -> new SubscriptionAwareAdapter(handler, custom));
		MethodRedisListenerEndpoint two = customEndpointFor(bean, "b", handler -> new SubscriptionAwareAdapter(handler, custom));

		MessageListener listener = registerAndStart(new BatchMethodRedisListenerEndpoint(List.of(one, two)));

		assertThat(listener).isInstanceOf(SubscriptionListener.class);

		((SubscriptionListener) listener).onChannelSubscribed("ch1".getBytes(), 1);
		verify(custom, times(1)).onChannelSubscribed("ch1".getBytes(), 1);

		listener.onMessage(new StringMessage(TOPIC, "hi"), null);
		assertThat(bean.invocations).containsExactlyInAnyOrder("a:hi", "b:hi");
	}

	@Test // GH-3439
	void shouldNotImplementSubscriptionListener() throws NoSuchMethodException {
		assertThat(registerAndStart(batchFor(new TestBean()))).isNotInstanceOf(SubscriptionListener.class);
	}

	@Test // GH-3439
	void shouldRegisterAndUnregisterOnce() throws NoSuchMethodException {

		BatchMethodRedisListenerEndpoint batch = batchFor(new TestBean());
		RedisMessageListenerContainer container = container();

		MessageListener listener = registerAndStart(batch, container);

		batch.stop();
		verify(container, times(1)).removeMessageListener(listener);
	}

	private MessageListener registerAndStart(BatchMethodRedisListenerEndpoint endpoint) {
		return registerAndStart(endpoint, container());
	}

	private static RedisMessageListenerContainer container() {

		RedisMessageListenerContainer container = mock(RedisMessageListenerContainer.class);
		when(container.getTopicSerializer()).thenReturn(RedisSerializer.string());
		return container;
	}

	private MessageListener registerAndStart(BatchMethodRedisListenerEndpoint endpoint,
			RedisMessageListenerContainer container) {

		ArgumentCaptor<MessageListener> listenerCaptor = ArgumentCaptor.forClass(MessageListener.class);

		endpoint.register(container);
		endpoint.start();

		verify(container, times(1)).addMessageListener(listenerCaptor.capture(), anyCollection());
		return listenerCaptor.getValue();
	}

	// batch of the bean's "a" and "b" methods, both on TOPIC
	private BatchMethodRedisListenerEndpoint batchFor(Object bean) throws NoSuchMethodException {
		return new BatchMethodRedisListenerEndpoint(List.of(endpointFor(bean, "a", TOPIC), endpointFor(bean, "b", TOPIC)));
	}

	private MethodRedisListenerEndpoint endpointFor(Object bean, String methodName, @Nullable String topic)
			throws NoSuchMethodException {

		Method method = bean.getClass().getMethod(methodName, String.class);
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(bean, method);
		endpoint.setTopic(topic);
		endpoint.setMessageHandlerMethodFactory(factory);
		return endpoint;
	}

	// endpoint on TOPIC whose listener is built by the given adapter factory, e.g. a throwing one
	private MethodRedisListenerEndpoint customEndpointFor(Object bean, String methodName,
			Function<InvocableHandlerMethod, HandlerMethodMessageListenerAdapter> adapter) throws NoSuchMethodException {

		Method method = bean.getClass().getMethod(methodName, String.class);
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(bean, method) {

			@Override
			public HandlerMethodMessageListenerAdapter createListener() {
				return adapter.apply(factory.createInvocableHandlerMethod(bean, method));
			}
		};
		endpoint.setTopic(TOPIC);
		return endpoint;
	}

	/**
	 * Adapter that fails every message with the given error message.
	 */
	static class ThrowingAdapter extends HandlerMethodMessageListenerAdapter {

		private final String errorMessage;

		ThrowingAdapter(InvocableHandlerMethod handlerMethod, String errorMessage) {
			super(handlerMethod, null);
			this.errorMessage = errorMessage;
		}

		@Override
		public void onMessage(Message message, byte @Nullable [] pattern) {
			throw new IllegalStateException(this.errorMessage);
		}

	}

	/**
	 * Custom adapter a {@link MethodRedisListenerEndpoint} subclass may return.
	 */
	static class SubscriptionAwareAdapter extends HandlerMethodMessageListenerAdapter implements SubscriptionListener {

		private final SubscriptionListener delegate;

		SubscriptionAwareAdapter(InvocableHandlerMethod handlerMethod, SubscriptionListener delegate) {
			super(handlerMethod, null);
			this.delegate = delegate;
		}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			this.delegate.onChannelSubscribed(channel, count);
		}

	}

	static class TestBean {

		final List<String> invocations = new CopyOnWriteArrayList<>();

		public void a(String message) {
			invocations.add("a:%s".formatted(message));
		}

		public void b(String message) {
			invocations.add("b:%s".formatted(message));
		}

		public void c(String message) {
			invocations.add("c:%s".formatted(message));
		}

	}

	static class SubscriptionAwareBean extends TestBean implements SubscriptionListener {

	}

	/**
	 * Topic serializer prefixing every topic, e.g. {@code ch1} becomes {@code app:ch1}.
	 */
	record PrefixingSerializer(String prefix) implements RedisSerializer<String> {

		@Override
		public byte[] serialize(@Nullable String value) {
			return RedisSerializer.string().serialize(this.prefix + value);
		}

		@Override
		public @Nullable String deserialize(byte @Nullable [] bytes) {
			return RedisSerializer.string().deserialize(bytes);
		}

	}

}
