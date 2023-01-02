/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.RawTargetAccess;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Helper class that provides static methods for obtaining {@link RedisConnection} from a
 * {@link RedisConnectionFactory}. Includes special support for Spring-managed transactional RedisConnections, e.g.
 * managed by {@link org.springframework.transaction.support.AbstractPlatformTransactionManager}..
 * <p>
 * Used internally by Spring's {@link RedisTemplate}. Can also be used directly in application code.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @see #getConnection
 * @see #releaseConnection
 * @see org.springframework.transaction.support.TransactionSynchronizationManager
 */
public abstract class RedisConnectionUtils {

	private static final Log log = LogFactory.getLog(RedisConnectionUtils.class);

	/**
	 * Obtain a {@link RedisConnection} from the given {@link RedisConnectionFactory} and binds the connection to the
	 * current thread to be used in closure-scope, if none is already bound. Considers ongoing transactions by reusing the
	 * transaction-bound connection and allows reentrant connection retrieval. Does not bind the connection to potentially
	 * ongoing transactions.
	 *
	 * @param factory connection factory
	 * @return a new Redis connection without transaction support.
	 */
	public static RedisConnection bindConnection(RedisConnectionFactory factory) {
		return doGetConnection(factory, true, true, false);
	}

	/**
	 * Obtain a {@link RedisConnection} from the given {@link RedisConnectionFactory} and binds the connection to the
	 * current thread to be used in closure-scope, if none is already bound. Considers ongoing transactions by reusing the
	 * transaction-bound connection and allows reentrant connection retrieval. Binds also the connection to the ongoing
	 * transaction if no connection is already bound if {@code transactionSupport} is enabled.
	 *
	 * @param factory connection factory.
	 * @param transactionSupport whether transaction support is enabled.
	 * @return a new Redis connection with transaction support if requested.
	 */
	public static RedisConnection bindConnection(RedisConnectionFactory factory, boolean transactionSupport) {
		return doGetConnection(factory, true, true, transactionSupport);
	}

	/**
	 * Obtain a {@link RedisConnection} from the given {@link RedisConnectionFactory}. Is aware of existing connections
	 * bound to the current transaction (when using a transaction manager) or the current thread (when binding a
	 * connection to a closure-scope). Does not bind newly created connections to ongoing transactions.
	 *
	 * @param factory connection factory for creating the connection.
	 * @return an active Redis connection without transaction management.
	 */
	public static RedisConnection getConnection(RedisConnectionFactory factory) {
		return getConnection(factory, false);
	}

	/**
	 * Obtain a {@link RedisConnection} from the given {@link RedisConnectionFactory}. Is aware of existing connections
	 * bound to the current transaction (when using a transaction manager) or the current thread (when binding a
	 * connection to a closure-scope).
	 *
	 * @param factory connection factory for creating the connection.
	 * @param transactionSupport whether transaction support is enabled.
	 * @return an active Redis connection with transaction management if requested.
	 */
	public static RedisConnection getConnection(RedisConnectionFactory factory, boolean transactionSupport) {
		return doGetConnection(factory, true, false, transactionSupport);
	}

	/**
	 * Actually obtain a {@link RedisConnection} from the given {@link RedisConnectionFactory}. Is aware of existing
	 * connections bound to the current transaction (when using a transaction manager) or the current thread (when binding
	 * a connection to a closure-scope). Will create a new {@link RedisConnection} otherwise, if {@code allowCreate} is
	 * {@literal true}. This method allows for re-entrance as {@link RedisConnectionHolder} keeps track of ref-count.
	 *
	 * @param factory connection factory for creating the connection.
	 * @param allowCreate whether a new (unbound) connection should be created when no connection can be found for the
	 *          current thread.
	 * @param bind binds the connection to the thread, in case one was created-
	 * @param transactionSupport whether transaction support is enabled.
	 * @return an active Redis connection.
	 */
	public static RedisConnection doGetConnection(RedisConnectionFactory factory, boolean allowCreate, boolean bind,
			boolean transactionSupport) {

		Assert.notNull(factory, "No RedisConnectionFactory specified");

		RedisConnectionHolder conHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(factory);

		if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
			conHolder.requested();
			if (!conHolder.hasConnection()) {
				log.debug("Fetching resumed Redis Connection from RedisConnectionFactory");
				conHolder.setConnection(fetchConnection(factory));
			}
			return conHolder.getRequiredConnection();
		}

		// Else we either got no holder or an empty thread-bound holder here.

		if (!allowCreate) {
			throw new IllegalArgumentException("No connection found and allowCreate = false");
		}

		log.debug("Fetching Redis Connection from RedisConnectionFactory");
		RedisConnection connection = fetchConnection(factory);

		boolean bindSynchronization = TransactionSynchronizationManager.isActualTransactionActive() && transactionSupport;

		if (bind || bindSynchronization) {

			if (bindSynchronization && isActualNonReadonlyTransactionActive()) {
				connection = createConnectionSplittingProxy(connection, factory);
			}

			try {
				// Use same RedisConnection for further Redis actions within the transaction.
				// Thread-bound object will get removed by synchronization at transaction completion.
				RedisConnectionHolder holderToUse = conHolder;
				if (holderToUse == null) {
					holderToUse = new RedisConnectionHolder(connection);
				} else {
					holderToUse.setConnection(connection);
				}
				holderToUse.requested();

				// Consider callback-scope connection binding vs. transaction scope binding
				if (bindSynchronization) {
					potentiallyRegisterTransactionSynchronisation(holderToUse, factory);
				}

				if (holderToUse != conHolder) {
					TransactionSynchronizationManager.bindResource(factory, holderToUse);
				}
			} catch (RuntimeException ex) {
				// Unexpected exception from external delegation call -> close Connection and rethrow.
				releaseConnection(connection, factory);
				throw ex;
			}

			return connection;
		}

		return connection;
	}

	/**
	 * Actually create a {@link RedisConnection} from the given {@link RedisConnectionFactory}.
	 *
	 * @param factory the {@link RedisConnectionFactory} to obtain RedisConnections from.
	 * @return a Redis Connection from the given {@link RedisConnectionFactory} (never {@literal null}).
	 * @see RedisConnectionFactory#getConnection()
	 */
	private static RedisConnection fetchConnection(RedisConnectionFactory factory) {
		return factory.getConnection();
	}

	private static void potentiallyRegisterTransactionSynchronisation(RedisConnectionHolder connHolder,
			final RedisConnectionFactory factory) {

		// Should go actually into RedisTransactionManager

		if (!connHolder.isTransactionActive()) {

			connHolder.setTransactionActive(true);
			connHolder.setSynchronizedWithTransaction(true);
			connHolder.requested();

			RedisConnection conn = connHolder.getRequiredConnection();
			boolean readOnly = TransactionSynchronizationManager.isCurrentTransactionReadOnly();
			if (!readOnly) {
				conn.multi();
			}

			TransactionSynchronizationManager
					.registerSynchronization(new RedisTransactionSynchronizer(connHolder, conn, factory, readOnly));
		}
	}

	private static boolean isActualNonReadonlyTransactionActive() {
		return TransactionSynchronizationManager.isActualTransactionActive()
				&& !TransactionSynchronizationManager.isCurrentTransactionReadOnly();
	}

	private static RedisConnection createConnectionSplittingProxy(RedisConnection connection,
			RedisConnectionFactory factory) {

		ProxyFactory proxyFactory = new ProxyFactory(connection);
		proxyFactory.addAdvice(new ConnectionSplittingInterceptor(factory));
		proxyFactory.addInterface(RedisConnectionProxy.class);

		return RedisConnection.class.cast(proxyFactory.getProxy());
	}

	/**
	 * Closes the given {@link RedisConnection}, created via the given factory if not managed externally (i.e. not bound
	 * to the transaction).
	 *
	 * @param conn the Redis connection to close.
	 * @param factory the Redis factory that the connection was created with.
	 */
	public static void releaseConnection(@Nullable RedisConnection conn, RedisConnectionFactory factory) {
		if (conn == null) {
			return;
		}

		RedisConnectionHolder conHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(factory);
		if (conHolder != null) {

			if (conHolder.isTransactionActive()) {
				if (connectionEquals(conHolder, conn)) {
					if (log.isDebugEnabled()) {
						log.debug("RedisConnection will be closed when transaction finished.");
					}

					// It's the transactional Connection: Don't close it.
					conHolder.released();
				}
				return;
			}

			// release transactional/read-only and non-transactional/non-bound connections.
			// transactional connections for read-only transactions get no synchronizer registered

			unbindConnection(factory);
			return;
		}

		doCloseConnection(conn);
	}

	/**
	 * Closes the given {@link RedisConnection}, created via the given factory if not managed externally (i.e. not bound
	 * to the transaction).
	 *
	 * @param conn the Redis connection to close.
	 * @param factory the Redis factory that the connection was created with.
	 * @param transactionSupport whether transaction support is enabled.
	 * @since 2.1.9
	 * @deprecated since 2.4.2, use {@link #releaseConnection(RedisConnection, RedisConnectionFactory)}
	 */
	@Deprecated
	public static void releaseConnection(@Nullable RedisConnection conn, RedisConnectionFactory factory,
			boolean transactionSupport) {
		releaseConnection(conn, factory);
	}

	/**
	 * Determine whether the given two RedisConnections are equal, asking the target {@link RedisConnection} in case of a
	 * proxy. Used to detect equality even if the user passed in a raw target Connection while the held one is a proxy.
	 *
	 * @param conHolder the {@link RedisConnectionHolder} for the held Connection (potentially a proxy)
	 * @param passedInCon the {@link RedisConnection} passed-in by the user (potentially a target Connection without
	 *          proxy)
	 * @return whether the given Connections are equal
	 * @see #getTargetConnection
	 */
	private static boolean connectionEquals(RedisConnectionHolder conHolder, RedisConnection passedInCon) {

		if (!conHolder.hasConnection()) {
			return false;
		}

		RedisConnection heldCon = conHolder.getRequiredConnection();

		return heldCon.equals(passedInCon) || getTargetConnection(heldCon).equals(passedInCon);
	}

	/**
	 * Return the innermost target {@link RedisConnection} of the given {@link RedisConnection}. If the given
	 * {@link RedisConnection} is a proxy, it will be unwrapped until a non-proxy {@link RedisConnection} is found.
	 * Otherwise, the passed-in {@link RedisConnection} will be returned as-is.
	 *
	 * @param con the {@link RedisConnection} proxy to unwrap
	 * @return the innermost target Connection, or the passed-in one if no proxy
	 * @see RedisConnectionProxy#getTargetConnection()
	 */
	private static RedisConnection getTargetConnection(RedisConnection con) {

		RedisConnection conToUse = con;

		while (conToUse instanceof RedisConnectionProxy) {
			conToUse = ((RedisConnectionProxy) conToUse).getTargetConnection();
		}

		return conToUse;
	}

	/**
	 * Unbinds and closes the connection (if any) associated with the given factory from closure-scope. Considers ongoing
	 * transactions so transaction-bound connections aren't closed and reentrant closure-scope bound connections. Only the
	 * outer-most call to leads to releasing and closing the connection.
	 *
	 * @param factory Redis factory
	 */
	public static void unbindConnection(RedisConnectionFactory factory) {

		RedisConnectionHolder conHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(factory);

		if (conHolder == null) {
			return;
		}

		if (log.isDebugEnabled()) {
			log.debug("Unbinding Redis Connection.");
		}

		if (conHolder.isTransactionActive()) {
			if (log.isDebugEnabled()) {
				log.debug("Redis Connection will be closed when outer transaction finished.");
			}
		} else {

			RedisConnection connection = conHolder.getConnection();
			conHolder.released();

			if (!conHolder.isOpen()) {

				TransactionSynchronizationManager.unbindResourceIfPossible(factory);

				doCloseConnection(connection);
			}
		}
	}

	/**
	 * Return whether the given Redis connection is transactional, that is, bound to the current thread by Spring's
	 * transaction facilities.
	 *
	 * @param conn Redis connection to check
	 * @param connFactory Redis connection factory that the connection was created with
	 * @return whether the connection is transactional or not
	 */
	public static boolean isConnectionTransactional(RedisConnection conn, RedisConnectionFactory connFactory) {

		Assert.notNull(connFactory, "No RedisConnectionFactory specified");

		RedisConnectionHolder connHolder = (RedisConnectionHolder) TransactionSynchronizationManager
				.getResource(connFactory);

		return connHolder != null && connectionEquals(connHolder, conn);
	}

	private static void doCloseConnection(@Nullable RedisConnection connection) {

		if (connection == null) {
			return;
		}

		if (log.isDebugEnabled()) {
			log.debug("Closing Redis Connection.");
		}

		try {
			connection.close();
		} catch (DataAccessException ex) {
			log.debug("Could not close Redis Connection", ex);
		} catch (Throwable ex) {
			log.debug("Unexpected exception on closing Redis Connection", ex);
		}
	}

	/**
	 * A {@link TransactionSynchronization} that makes sure that the associated {@link RedisConnection} is released after
	 * the transaction completes.
	 *
	 * @author Christoph Strobl
	 * @author Thomas Darimont
	 * @author Mark Paluch
	 */
	private static class RedisTransactionSynchronizer implements TransactionSynchronization {

		private final RedisConnectionHolder connHolder;
		private final RedisConnection connection;
		private final RedisConnectionFactory factory;
		private final boolean readOnly;

		RedisTransactionSynchronizer(RedisConnectionHolder connHolder, RedisConnection connection,
				RedisConnectionFactory factory, boolean readOnly) {

			this.connHolder = connHolder;
			this.connection = connection;
			this.factory = factory;
			this.readOnly = readOnly;
		}

		@Override
		public void afterCompletion(int status) {

			try {
				if (!readOnly) {
					switch (status) {

						case TransactionSynchronization.STATUS_COMMITTED:
							connection.exec();
							break;

						case TransactionSynchronization.STATUS_ROLLED_BACK:
						case TransactionSynchronization.STATUS_UNKNOWN:
						default:
							connection.discard();
					}
				}
			} finally {

				if (log.isDebugEnabled()) {
					log.debug("Closing bound connection after transaction completed with " + status);
				}

				connHolder.setTransactionActive(false);
				doCloseConnection(connection);
				TransactionSynchronizationManager.unbindResource(factory);
				connHolder.reset();
			}
		}
	}

	/**
	 * {@link MethodInterceptor} that invokes read-only commands on a new {@link RedisConnection} while read-write
	 * commands are queued on the bound connection.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.3
	 */
	static class ConnectionSplittingInterceptor
			implements MethodInterceptor {

		private final RedisConnectionFactory factory;

		public ConnectionSplittingInterceptor(RedisConnectionFactory factory) {
			this.factory = factory;
		}

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments());
		}

		public Object intercept(Object obj, Method method, Object[] args) throws Throwable {

			if (method.getName().equals("getTargetConnection")) {
				// Handle getTargetConnection method: return underlying RedisConnection.
				return obj;
			}

			RedisCommand commandToExecute = RedisCommand.failsafeCommandLookup(method.getName());

			if (isPotentiallyThreadBoundCommand(commandToExecute)) {

				if (log.isDebugEnabled()) {
					log.debug(String.format("Invoke '%s' on bound connection", method.getName()));
				}

				return invoke(method, obj, args);
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Invoke '%s' on unbound connection", method.getName()));
			}

			RedisConnection connection = factory.getConnection();

			try {
				return invoke(method, connection, args);
			} finally {
				// properly close the unbound connection after executing command
				if (!connection.isClosed()) {
					doCloseConnection(connection);
				}
			}
		}

		private Object invoke(Method method, Object target, Object[] args) throws Throwable {

			try {
				return method.invoke(target, args);
			} catch (InvocationTargetException e) {
				throw e.getCause();
			}
		}


		private boolean isPotentiallyThreadBoundCommand(RedisCommand command) {
			return RedisCommand.UNKNOWN.equals(command) || !command.isReadonly();
		}
	}

	/**
	 * Resource holder wrapping a {@link RedisConnection}. {@link RedisConnectionUtils} binds instances of this class to
	 * the thread, for a specific {@link RedisConnectionFactory}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	private static class RedisConnectionHolder extends ResourceHolderSupport {

		@Nullable private RedisConnection connection;

		private boolean transactionActive = false;

		/**
		 * Create a new RedisConnectionHolder for the given Redis Connection assuming that there is no ongoing transaction.
		 *
		 * @param connection the Redis Connection to hold.
		 * @see #RedisConnectionHolder(RedisConnection, boolean)
		 */
		public RedisConnectionHolder(RedisConnection connection) {
			this.connection = connection;
		}

		/**
		 * Return whether this holder currently has a {@link RedisConnection}.
		 */
		protected boolean hasConnection() {
			return (this.connection != null);
		}

		@Nullable
		public RedisConnection getConnection() {
			return connection;
		}

		public RedisConnection getRequiredConnection() {

			RedisConnection connection = getConnection();

			if (connection == null) {
				throw new IllegalStateException("No active RedisConnection");
			}

			return connection;
		}

		/**
		 * Override the existing {@link RedisConnection} handle with the given {@link RedisConnection}. Reset the handle if
		 * given {@literal null}.
		 * <p>
		 * Used for releasing the Connection on suspend (with a {@code null} argument) and setting a fresh Connection on
		 * resume.
		 */
		protected void setConnection(@Nullable RedisConnection connection) {
			this.connection = connection;
		}

		/**
		 * Set whether this holder represents an active, managed transaction.
		 *
		 * @see org.springframework.transaction.PlatformTransactionManager
		 */
		protected void setTransactionActive(boolean transactionActive) {
			this.transactionActive = transactionActive;
		}

		/**
		 * Return whether this holder represents an active, managed transaction.
		 */
		protected boolean isTransactionActive() {
			return this.transactionActive;
		}

		/**
		 * Releases the current Connection held by this ConnectionHolder.
		 * <p>
		 * This is necessary for ConnectionHandles that expect "Connection borrowing", where each returned Connection is
		 * only temporarily leased and needs to be returned once the data operation is done, to make the Connection
		 * available for other operations within the same transaction.
		 */
		@Override
		public void released() {
			super.released();
			if (!isOpen()) {
				setConnection(null);
			}
		}

		@Override
		public void clear() {
			super.clear();
			this.transactionActive = false;
		}
	}

	/**
	 * Subinterface of {@link RedisConnection} to be implemented by {@link RedisConnection} proxies. Allows access to the
	 * underlying target {@link RedisConnection}.
	 *
	 * @since 2.4.2
	 * @see RedisConnectionUtils#getTargetConnection(RedisConnection)
	 */
	public interface RedisConnectionProxy extends RedisConnection, RawTargetAccess {

		/**
		 * Return the target {@link RedisConnection} of this proxy.
		 * <p>
		 * This will typically be the native driver {@link RedisConnection} or a wrapper from a connection pool.
		 *
		 * @return the underlying {@link RedisConnection} (never {@link null}).
		 */
		RedisConnection getTargetConnection();

	}
}
