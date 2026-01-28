package org.springframework.data.redis.annotation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.Message;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class MessagingMessageListenerAdapterTest {
	@Mock private Message message;

	@Test // GH-1004
	void shouldConvertBytesToStringAndInvokeMethod() throws NoSuchMethodException {
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter();

		TestDelegate delegate = new TestDelegate();
		Method method = TestDelegate.class.getMethod("handleString", String.class);
		adapter.setHandlerMethod(delegate, method);

		byte[] payload = "Hello World".getBytes(StandardCharsets.UTF_8);
		given(message.getBody()).willReturn(payload);
		adapter.onMessage(message, null);
		assertThat(delegate.capturedPayload).isEqualTo("Hello World");
	}

	@Test // GH-1004
	void shouldPassRawBytes_WhenArgumentIsByteArray() throws NoSuchMethodException {
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter();

		TestDelegate delegate = new TestDelegate();
		Method method = TestDelegate.class.getMethod("handleBytes", byte[].class);
		adapter.setHandlerMethod(delegate, method);

		byte[] payload = { 1, 2, 3 };
		given(message.getBody()).willReturn(payload);
		adapter.onMessage(message, null);
		assertThat(delegate.capturedBytes).isEqualTo(payload);
	}

	static class TestDelegate {
		String capturedPayload;
		byte[] capturedBytes;

		public void handleString(String payload) {
			this.capturedPayload = payload;
		}

		public void handleBytes(byte[] payload) {
			this.capturedBytes = payload;
		}
	}
}
