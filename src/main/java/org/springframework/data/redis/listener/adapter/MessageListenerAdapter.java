/*
 * Copyright 2011-2017 the original author or authors.
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodFilter;
import org.springframework.util.StringUtils;

/**
 * Message listener adapter that delegates the handling of messages to target listener methods via reflection, with
 * flexible message type conversion. Allows listener methods to operate on message content types, completely independent
 * from the Redis API.
 * <p/>
 * Make sure to call {@link #afterPropertiesSet()} after setting all the parameters on the adapter.
 * <p/>
 * Note that if the underlying "delegate" is implementing {@link MessageListener}, the adapter will delegate to it and
 * allow an invalid method to be specified. However if it is not, the method becomes mandatory. This lenient behavior
 * allows the adapter to be used uniformly across existing listeners and message POJOs.
 * <p/>
 * Modeled as much as possible after the JMS MessageListenerAdapter in Spring Framework.
 * <p>
 * By default, the content of incoming Redis messages gets extracted before being passed into the target listener
 * method, to let the target method operate on message content types such as String or byte array instead of the raw
 * {@link Message}. Message type conversion is delegated to a Spring Data {@link RedisSerializer}. By default, the
 * {@link JdkSerializationRedisSerializer} will be used. (If you do not want such automatic message conversion taking
 * place, then be sure to set the {@link #setSerializer Serializer} to <code>null</code>.)
 * <p>
 * Find below some examples of method signatures compliant with this adapter class. This first example handles all
 * <code>Message</code> types and gets passed the contents of each <code>Message</code> type as an argument.
 *
 * <pre class="code">
 * public interface MessageContentsDelegate {
 * 	void handleMessage(String text);
 *
 * 	void handleMessage(byte[] bytes);
 *
 * 	void handleMessage(Person obj);
 * }
 * </pre>
 * <p>
 * In addition, the channel or pattern to which a message is sent can be passed in to the method as a second argument of
 * type String:
 *
 * <pre class="code">
 * public interface MessageContentsDelegate {
 * 	void handleMessage(String text, String channel);
 *
 * 	void handleMessage(byte[] bytes, String pattern);
 * }
 * </pre>
 *
 * For further examples and discussion please do refer to the Spring Data reference documentation which describes this
 * class (and its attendant configuration) in detail. <b>Important:</b> Due to the nature of messages, the default
 * serializer used by the adapter is {@link StringRedisSerializer}. If the messages are of a different type, change them
 * accordingly through {@link #setSerializer(RedisSerializer)}.
 *
 * @author Juergen Hoeller
 * @author Costin Leau
 * @author Greg Turnquist
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see org.springframework.jms.listener.adapter.MessageListenerAdapter
 */
public class MessageListenerAdapter implements InitializingBean, MessageListener {

	// TODO move this down.
	private class MethodInvoker {

		private final Object delegate;
		private String methodName;
		private Set<Method> methods;
		private boolean lenient;

		MethodInvoker(Object delegate, String methodName) {

			this.delegate = delegate;
			this.methodName = methodName;
			this.lenient = delegate instanceof MessageListener;
			this.methods = new HashSet<>();

			Class<?> c = delegate.getClass();

			ReflectionUtils.doWithMethods(c, method -> {
				ReflectionUtils.makeAccessible(method);
				methods.add(method);
			}, new MostSpecificMethodFilter(methodName, c));

			Assert.isTrue(lenient || !methods.isEmpty(), "Cannot find a suitable method named [" + c.getName() + "#"
					+ methodName + "] - is the method public and has the proper arguments?");
		}

		void invoke(Object[] arguments) throws InvocationTargetException, IllegalAccessException {

			Object[] message = new Object[] { arguments[0] };

			for (Method m : methods) {

				Class<?>[] types = m.getParameterTypes();
				Object[] args = //
						types.length == 2 //
								&& types[0].isInstance(arguments[0]) //
								&& types[1].isInstance(arguments[1]) ? arguments : message;

				if (!types[0].isInstance(args[0])) {
					continue;
				}

				m.invoke(delegate, args);

				return;
			}
		}

		/**
		 * Returns the current methodName.
		 *
		 * @return the methodName
		 */
		public String getMethodName() {
			return methodName;
		}
	}

	/**
	 * Out-of-the-box value for the default listener method: "handleMessage".
	 */
	public static final String ORIGINAL_DEFAULT_LISTENER_METHOD = "handleMessage";

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private volatile @Nullable Object delegate;

	private volatile @Nullable MethodInvoker invoker;

	private String defaultListenerMethod = ORIGINAL_DEFAULT_LISTENER_METHOD;

	private @Nullable RedisSerializer<?> serializer;

	private @Nullable RedisSerializer<String> stringSerializer;

	/**
	 * Create a new {@link MessageListenerAdapter} with default settings.
	 */
	public MessageListenerAdapter() {
		initDefaultStrategies();
		this.delegate = this;
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 *
	 * @param delegate the delegate object
	 */
	public MessageListenerAdapter(Object delegate) {
		initDefaultStrategies();
		setDelegate(delegate);
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 *
	 * @param delegate the delegate object
	 * @param defaultListenerMethod method to call when a message comes
	 * @see #getListenerMethodName
	 */
	public MessageListenerAdapter(Object delegate, String defaultListenerMethod) {
		this(delegate);
		setDefaultListenerMethod(defaultListenerMethod);
	}

	/**
	 * Set a target object to delegate message listening to. Specified listener methods have to be present on this target
	 * object.
	 * <p>
	 * If no explicit delegate object has been specified, listener methods are expected to present on this adapter
	 * instance, that is, on a custom subclass of this adapter, defining listener methods.
	 *
	 * @param delegate delegate object
	 */
	public void setDelegate(Object delegate) {
		Assert.notNull(delegate, "Delegate must not be null");
		this.delegate = delegate;
	}

	/**
	 * Returns the target object to delegate message listening to.
	 *
	 * @return message listening delegation
	 */
	@Nullable
	public Object getDelegate() {
		return this.delegate;
	}

	/**
	 * Specify the name of the default listener method to delegate to, for the case where no specific listener method has
	 * been determined. Out-of-the-box value is {@link #ORIGINAL_DEFAULT_LISTENER_METHOD "handleMessage"}.
	 *
	 * @see #getListenerMethodName
	 */
	public void setDefaultListenerMethod(String defaultListenerMethod) {
		this.defaultListenerMethod = defaultListenerMethod;
	}

	/**
	 * Return the name of the default listener method to delegate to.
	 */
	protected String getDefaultListenerMethod() {
		return this.defaultListenerMethod;
	}

	/**
	 * Set the serializer that will convert incoming raw Redis messages to listener method arguments.
	 * <p>
	 * The default converter is a {@link StringRedisSerializer}.
	 *
	 * @param serializer
	 */
	public void setSerializer(RedisSerializer<?> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Sets the serializer used for converting the channel/pattern to a String.
	 * <p>
	 * The default converter is a {@link StringRedisSerializer}.
	 *
	 * @param serializer
	 */
	public void setStringSerializer(RedisSerializer<String> serializer) {
		this.stringSerializer = serializer;
	}

	public void afterPropertiesSet() {
		String methodName = getDefaultListenerMethod();

		if (!StringUtils.hasText(methodName)) {
			throw new InvalidDataAccessApiUsageException("No default listener method specified: "
					+ "Either specify a non-null value for the 'defaultListenerMethod' property or "
					+ "override the 'getListenerMethodName' method.");
		}

		invoker = new MethodInvoker(delegate, methodName);
	}

	/**
	 * Standard Redis {@link MessageListener} entry point.
	 * <p>
	 * Delegates the message to the target listener method, with appropriate conversion of the message argument. In case
	 * of an exception, the {@link #handleListenerException(Throwable)} method will be invoked.
	 *
	 * @param message the incoming Redis message
	 * @see #handleListenerException
	 */
	@Override
	public void onMessage(Message message, @Nullable byte[] pattern) {
		try {
			// Check whether the delegate is a MessageListener impl itself.
			// In that case, the adapter will simply act as a pass-through.
			if (delegate != this) {
				if (delegate instanceof MessageListener) {
					((MessageListener) delegate).onMessage(message, pattern);
					return;
				}
			}

			// Regular case: find a handler method reflectively.
			Object convertedMessage = extractMessage(message);
			String convertedChannel = stringSerializer.deserialize(pattern);
			// Invoke the handler method with appropriate arguments.
			Object[] listenerArguments = new Object[] { convertedMessage, convertedChannel };

			invokeListenerMethod(invoker.getMethodName(), listenerArguments);
		} catch (Throwable th) {
			handleListenerException(th);
		}
	}

	/**
	 * Initialize the default implementations for the adapter's strategies.
	 *
	 * @see #setSerializer(RedisSerializer)
	 * @see JdkSerializationRedisSerializer
	 */
	protected void initDefaultStrategies() {
		setSerializer(StringRedisSerializer.UTF_8);
		setStringSerializer(StringRedisSerializer.UTF_8);
	}

	/**
	 * Handle the given exception that arose during listener execution. The default implementation logs the exception at
	 * error level.
	 *
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Redis message.
	 *
	 * @param message the Redis <code>Message</code>
	 * @return the content of the message, to be passed into the listener method as argument
	 */
	protected Object extractMessage(Message message) {
		if (serializer != null) {
			return serializer.deserialize(message.getBody());
		}
		return message.getBody();
	}

	/**
	 * Determine the name of the listener method that is supposed to handle the given message.
	 * <p>
	 * The default implementation simply returns the configured default listener method, if any.
	 *
	 * @param originalMessage the Redis request message
	 * @param extractedMessage the converted Redis request message, to be passed into the listener method as argument
	 * @return the name of the listener method (never <code>null</code>)
	 * @see #setDefaultListenerMethod
	 */
	protected String getListenerMethodName(Message originalMessage, Object extractedMessage) {
		return getDefaultListenerMethod();
	}

	/**
	 * Invoke the specified listener method.
	 *
	 * @param methodName the name of the listener method
	 * @param arguments the message arguments to be passed in
	 * @see #getListenerMethodName
	 */
	protected void invokeListenerMethod(String methodName, Object[] arguments) {
		try {
			invoker.invoke(arguments);
		} catch (InvocationTargetException ex) {
			Throwable targetEx = ex.getTargetException();
			if (targetEx instanceof DataAccessException) {
				throw (DataAccessException) targetEx;
			} else {
				throw new RedisListenerExecutionFailedException("Listener method '" + methodName + "' threw exception",
						targetEx);
			}
		} catch (Throwable ex) {
			throw new RedisListenerExecutionFailedException("Failed to invoke target method '" + methodName
					+ "' with arguments " + ObjectUtils.nullSafeToString(arguments), ex);
		}
	}

	/**
	 * @since 1.4
	 */
	static final class MostSpecificMethodFilter implements MethodFilter {

		private final String methodName;
		private final Class<?> c;

		MostSpecificMethodFilter(String methodName, Class<?> c) {

			this.methodName = methodName;
			this.c = c;
		}

		public boolean matches(Method method) {

			if (Modifier.isPublic(method.getModifiers()) //
					&& methodName.equals(method.getName()) //
					&& method.equals(ClassUtils.getMostSpecificMethod(method, c))) {

				// check out the argument numbers
				Class<?>[] parameterTypes = method.getParameterTypes();

				return ((parameterTypes.length == 2 && String.class.equals(parameterTypes[1])) || parameterTypes.length == 1);
			}

			return false;
		}
	}
}
