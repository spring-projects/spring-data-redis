/*
 * Copyright 2002-2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.listener.adapter;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.keyvalue.redis.connection.Message;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;

/**
 * Message listener adapter that delegates the handling of messages to target
 * listener methods via reflection, with flexible message type conversion.
 * Allows listener methods to operate on message content types, completely
 * independent from the Redis API.
 * 
 * <p/>Modeled as much as possible after the JMS MessageListenerAdapter in
 * Spring Framework.
 *
 * <p>By default, the content of incoming Redis messages gets extracted before
 * being passed into the target listener method, to let the target method
 * operate on message content types such as String or byte array instead of
 * the raw {@link Message}. Message type conversion is delegated to a Spring
 * Data {@link RedisSerializer}. By default, the {@link JdkSerializationRedisSerializer}
 * will be used. (If you do not want such automatic message conversion taking
 * place, then be sure to set the {@link #setSerializer Serializer}
 * to <code>null</code>.)
 *
 * <p>Find below some examples of method signatures compliant with this
 * adapter class. This first example handles all <code>Message</code> types
 * and gets passed the contents of each <code>Message</code> type as an
 * argument.
 *
 * <pre class="code">public interface MessageContentsDelegate {
 *    void handleMessage(String text);
 *    void handleMessage(byte[] bytes);
 *    void handleMessage(Person obj);
 * }</pre>
 *
 * For further examples and discussion please do refer to the Spring Data
 * reference documentation which describes this class (and it's attendant
 * XML configuration) in detail.
 *
 * @author Juergen Hoeller
 * @author Costin Leau
 * @see org.springframework.jms.listener.adapter.MessageListenerAdapter
 */
public class MessageListenerAdapter implements MessageListener {

	/**
	 * Out-of-the-box value for the default listener method: "handleMessage".
	 */
	public static final String ORIGINAL_DEFAULT_LISTENER_METHOD = "handleMessage";


	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private Object delegate;

	private String defaultListenerMethod = ORIGINAL_DEFAULT_LISTENER_METHOD;

	private RedisSerializer<?> serializer;


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
	 * Set a target object to delegate message listening to.
	 * Specified listener methods have to be present on this target object.
	 * <p>If no explicit delegate object has been specified, listener
	 * methods are expected to present on this adapter instance, that is,
	 * on a custom subclass of this adapter, defining listener methods.
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
	public Object getDelegate() {
		return this.delegate;
	}

	/**
	 * Specify the name of the default listener method to delegate to,
	 * for the case where no specific listener method has been determined.
	 * Out-of-the-box value is {@link #ORIGINAL_DEFAULT_LISTENER_METHOD "handleMessage"}.
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
	 * Set the serializer that will convert incoming raw Redis messages to
	 * listener method arguments.
	 * <p>The default converter is a {@link JdkSerializationRedisSerializer}, which is able
	 * to handle {@link Serializable} objects.
	 */
	public void setSerializer(RedisSerializer<?> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Standard Redis {@link MessageListener} entry point.
	 * <p>Delegates the message to the target listener method, with appropriate
	 * conversion of the message argument. In case of an exception, the
	 * {@link #handleListenerException(Throwable)} method will be invoked.
	 * 
	 * @param message the incoming Redis message
	 * @see #handleListenerException
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void onMessage(Message message, byte[] pattern) {
		try {

			// Check whether the delegate is a MessageListener impl itself.
			// In that case, the adapter will simply act as a pass-through.
			if (delegate != this) {
				if (delegate instanceof MessageListener) {
					((MessageListener) delegate).onMessage(message, pattern);
				}
			}

			// Regular case: find a handler method reflectively.
			Object convertedMessage = extractMessage(message);
			String methodName = getListenerMethodName(message, convertedMessage);
			if (methodName == null) {
				throw new InvalidDataAccessApiUsageException("No default listener method specified: "
						+ "Either specify a non-null value for the 'defaultListenerMethod' property or "
						+ "override the 'getListenerMethodName' method.");
			}

			// Invoke the handler method with appropriate arguments.
			Object[] listenerArguments = buildListenerArguments(convertedMessage);
			invokeListenerMethod(methodName, listenerArguments);
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
		setSerializer(new JdkSerializationRedisSerializer());
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * The default implementation logs the exception at error level.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Redis message.
	 * @param message the Redis <code>Message</code>
	 * @return the content of the message, to be passed into the
	 * listener method as argument
	 */
	protected Object extractMessage(Message message) {
		if (serializer != null) {
			return serializer.deserialize(message.getPayload());
		}
		return message;
	}

	/**
	 * Determine the name of the listener method that is supposed to
	 * handle the given message.
	 * <p>The default implementation simply returns the configured
	 * default listener method, if any.
	 * @param originalMessage the Redis request message
	 * @param extractedMessage the converted Redis request message,
	 * to be passed into the listener method as argument
	 * @return the name of the listener method (never <code>null</code>)
	 * @see #setDefaultListenerMethod
	 */
	protected String getListenerMethodName(Message originalMessage, Object extractedMessage) {
		return getDefaultListenerMethod();
	}

	/**
	 * Build an array of arguments to be passed into the target listener method.
	 * Allows for multiple method arguments to be built from a single message object.
	 * <p>The default implementation builds an array with the given message object
	 * as sole element. This means that the extracted message will always be passed
	 * into a <i>single</i> method argument, even if it is an array, with the target
	 * method having a corresponding single argument of the array's type declared.
	 * <p>This can be overridden to treat special message content such as arrays
	 * differently, for example passing in each element of the message array
	 * as distinct method argument.
	 * @param extractedMessage the content of the message
	 * @return the array of arguments to be passed into the
	 * listener method (each element of the array corresponding
	 * to a distinct method argument)
	 */
	protected Object[] buildListenerArguments(Object extractedMessage) {
		return new Object[] { extractedMessage };
	}

	/**
	 * Invoke the specified listener method.
	 * @param methodName the name of the listener method
	 * @param arguments the message arguments to be passed in
	 * @return the result returned from the listener method
	 * @see #getListenerMethodName
	 * @see #buildListenerArguments
	 */
	protected Object invokeListenerMethod(String methodName, Object[] arguments) {
		try {
			MethodInvoker methodInvoker = new MethodInvoker();
			methodInvoker.setTargetObject(getDelegate());
			methodInvoker.setTargetMethod(methodName);
			methodInvoker.setArguments(arguments);
			methodInvoker.prepare();
			return methodInvoker.invoke();
		} catch (InvocationTargetException ex) {
			Throwable targetEx = ex.getTargetException();
			if (targetEx instanceof DataAccessException) {
				throw (DataAccessException) targetEx;
			}
			else {
				throw new ListenerExecutionFailedException("Listener method '" + methodName + "' threw exception",
						targetEx);
			}
		} catch (Throwable ex) {
			throw new ListenerExecutionFailedException("Failed to invoke target method '" + methodName
					+ "' with arguments " + ObjectUtils.nullSafeToString(arguments), ex);
		}
	}
}