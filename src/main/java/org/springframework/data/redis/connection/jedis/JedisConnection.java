/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.GeoCoordinate;
import org.springframework.data.redis.core.GeoRadiusResponse;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.*;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Pool;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/xetorthio/jedis">Jedis</a> library.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Jungtaek Lim
 * @author Konstantin Shchepanovskyi
 * @author David Liu
 * @author Milan Agatonovic
 * @author Mark Paluch
 */
public class JedisConnection extends AbstractRedisConnection {

    private static final Field CLIENT_FIELD;
    private static final Method SEND_COMMAND;
    private static final Method GET_RESPONSE;

    private static final String SHUTDOWN_SCRIPT = "return redis.call('SHUTDOWN','%s')";

    private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
            JedisConverters.exceptionConverter());

    static {

        CLIENT_FIELD = ReflectionUtils.findField(BinaryJedis.class, "client", Client.class);
        ReflectionUtils.makeAccessible(CLIENT_FIELD);

        try {
            Class<?> commandType = ClassUtils.isPresent("redis.clients.jedis.ProtocolCommand", null) ? ClassUtils.forName(
                    "redis.clients.jedis.ProtocolCommand", null) : ClassUtils.forName("redis.clients.jedis.Protocol$Command",
                    null);

            SEND_COMMAND = ReflectionUtils.findMethod(Connection.class, "sendCommand", new Class[]{commandType,
                    byte[][].class});
        } catch (Exception e) {
            throw new NoClassDefFoundError(
                    "Could not find required flavor of command required by 'redis.clients.jedis.Connection#sendCommand'.");
        }

        ReflectionUtils.makeAccessible(SEND_COMMAND);

        GET_RESPONSE = ReflectionUtils.findMethod(Queable.class, "getResponse", Builder.class);
        ReflectionUtils.makeAccessible(GET_RESPONSE);
    }

    private final Jedis jedis;
    private final Client client;
    private Transaction transaction;
    private final Pool<Jedis> pool;
    /**
     * flag indicating whether the connection needs to be dropped or not
     */
    private boolean broken = false;
    private volatile JedisSubscription subscription;
    private volatile Pipeline pipeline;
    private final int dbIndex;
    private boolean convertPipelineAndTxResults = true;
    private List<FutureResult<Response<?>>> pipelinedResults = new ArrayList<FutureResult<Response<?>>>();
    private Queue<FutureResult<Response<?>>> txResults = new LinkedList<FutureResult<Response<?>>>();

    private class JedisResult extends FutureResult<Response<?>> {
        public <T> JedisResult(Response<T> resultHolder, Converter<T, ?> converter) {
            super(resultHolder, converter);
        }

        public <T> JedisResult(Response<T> resultHolder) {
            super(resultHolder);
        }

        @SuppressWarnings("unchecked")
        public Object get() {
            if (convertPipelineAndTxResults && converter != null) {
                return converter.convert(resultHolder.get());
            }
            return resultHolder.get();
        }
    }

    private class JedisStatusResult extends JedisResult {
        public JedisStatusResult(Response<?> resultHolder) {
            super(resultHolder);
            setStatus(true);
        }
    }

    /**
     * Constructs a new <code>JedisConnection</code> instance.
     *
     * @param jedis Jedis entity
     */
    public JedisConnection(Jedis jedis) {
        this(jedis, null, 0);
    }

    /**
     * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
     *
     * @param jedis
     * @param pool  can be null, if no pool is used
     */
    public JedisConnection(Jedis jedis, Pool<Jedis> pool, int dbIndex) {
        this.jedis = jedis;
        // extract underlying connection for batch operations
        client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);

        this.pool = pool;

        this.dbIndex = dbIndex;

        // select the db
        // if this fail, do manual clean-up before propagating the exception
        // as we're inside the constructor
        if (dbIndex > 0) {
            try {
                select(dbIndex);
            } catch (DataAccessException ex) {
                close();
                throw ex;
            }
        }
    }

    protected DataAccessException convertJedisAccessException(Exception ex) {

        if (ex instanceof NullPointerException) {
            // An NPE before flush will leave data in the OutputStream of a pooled connection
            broken = true;
        }

        DataAccessException exception = EXCEPTION_TRANSLATION.translate(ex);
        if (exception instanceof RedisConnectionFailureException) {
            broken = true;
        }

        return exception;
    }

    public Object execute(String command, byte[]... args) {
        Assert.hasText(command, "a valid command needs to be specified");
        try {
            List<byte[]> mArgs = new ArrayList<byte[]>();
            if (!ObjectUtils.isEmpty(args)) {
                Collections.addAll(mArgs, args);
            }

            ReflectionUtils.invokeMethod(SEND_COMMAND, client, Command.valueOf(command.trim().toUpperCase()),
                    mArgs.toArray(new byte[mArgs.size()][]));
            if (isQueueing() || isPipelined()) {
                Object target = (isPipelined() ? pipeline : transaction);
                @SuppressWarnings("unchecked")
                Response<Object> result = (Response<Object>) ReflectionUtils.invokeMethod(GET_RESPONSE, target,
                        new Builder<Object>() {
                            public Object build(Object data) {
                                return data;
                            }

                            public String toString() {
                                return "Object";
                            }
                        });
                if (isPipelined()) {
                    pipeline(new JedisResult(result));
                } else {
                    transaction(new JedisResult(result));
                }
                return null;
            }
            return client.getOne();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void close() throws DataAccessException {
        super.close();
        // return the connection to the pool
        if (pool != null) {
            if (!broken) {
                // reset the connection
                try {
                    if (dbIndex > 0) {
                        jedis.select(0);
                    }
                    pool.returnResource(jedis);
                    return;
                } catch (Exception ex) {
                    DataAccessException dae = convertJedisAccessException(ex);
                    if (broken) {
                        pool.returnBrokenResource(jedis);
                    } else {
                        pool.returnResource(jedis);
                    }
                    throw dae;
                }
            } else {
                pool.returnBrokenResource(jedis);
                return;
            }
        }
        // else close the connection normally (doing the try/catch dance)
        Exception exc = null;
        if (isQueueing()) {
            try {
                client.quit();
            } catch (Exception ex) {
                exc = ex;
            }
            try {
                client.disconnect();
            } catch (Exception ex) {
                exc = ex;
            }
            return;
        }
        try {
            jedis.quit();
        } catch (Exception ex) {
            exc = ex;
        }
        try {
            jedis.disconnect();
        } catch (Exception ex) {
            exc = ex;
        }
        if (exc != null)
            throw convertJedisAccessException(exc);
    }

    public Jedis getNativeConnection() {
        return jedis;
    }

    public boolean isClosed() {
        try {
            return !jedis.isConnected();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public boolean isQueueing() {
        return client.isInMulti();
    }

    public boolean isPipelined() {
        return (pipeline != null);
    }

    public void openPipeline() {
        if (pipeline == null) {
            pipeline = jedis.pipelined();
        }
    }

    public List<Object> closePipeline() {
        if (pipeline != null) {
            try {
                return convertPipelineResults();
            } finally {
                pipeline = null;
                pipelinedResults.clear();
            }
        }
        return Collections.emptyList();
    }

    private List<Object> convertPipelineResults() {
        List<Object> results = new ArrayList<Object>();
        pipeline.sync();
        Exception cause = null;
        for (FutureResult<Response<?>> result : pipelinedResults) {
            try {
                Object data = result.get();
                if (!convertPipelineAndTxResults || !(result.isStatus())) {
                    results.add(data);
                }
            } catch (JedisDataException e) {
                DataAccessException dataAccessException = convertJedisAccessException(e);
                if (cause == null) {
                    cause = dataAccessException;
                }
                results.add(dataAccessException);
            } catch (DataAccessException e) {
                if (cause == null) {
                    cause = e;
                }
                results.add(e);
            }
        }
        if (cause != null) {
            throw new RedisPipelineException(cause, results);
        }
        return results;
    }

    private void doPipelined(Response<?> response) {
        pipeline(new JedisStatusResult(response));
    }

    private void pipeline(FutureResult<Response<?>> result) {
        if (isQueueing()) {
            transaction(result);
        } else {
            pipelinedResults.add(result);
        }
    }

    private void doQueued(Response<?> response) {
        transaction(new JedisStatusResult(response));
    }

    private void transaction(FutureResult<Response<?>> result) {
        txResults.add(result);
    }

    public List<byte[]> sort(byte[] key, SortParameters params) {

        SortingParams sortParams = JedisConverters.toSortingParams(params);

        try {
            if (isPipelined()) {
                if (sortParams != null) {
                    pipeline(new JedisResult(pipeline.sort(key, sortParams)));
                } else {
                    pipeline(new JedisResult(pipeline.sort(key)));
                }

                return null;
            }
            if (isQueueing()) {
                if (sortParams != null) {
                    transaction(new JedisResult(transaction.sort(key, sortParams)));
                } else {
                    transaction(new JedisResult(transaction.sort(key)));
                }

                return null;
            }
            return (sortParams != null ? jedis.sort(key, sortParams) : jedis.sort(key));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

        SortingParams sortParams = JedisConverters.toSortingParams(params);

        try {
            if (isPipelined()) {
                if (sortParams != null) {
                    pipeline(new JedisResult(pipeline.sort(key, sortParams, storeKey)));
                } else {
                    pipeline(new JedisResult(pipeline.sort(key, storeKey)));
                }

                return null;
            }
            if (isQueueing()) {
                if (sortParams != null) {
                    transaction(new JedisResult(transaction.sort(key, sortParams, storeKey)));
                } else {
                    transaction(new JedisResult(transaction.sort(key, storeKey)));
                }

                return null;
            }
            return (sortParams != null ? jedis.sort(key, sortParams, storeKey) : jedis.sort(key, storeKey));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long dbSize() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.dbSize()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.dbSize()));
                return null;
            }
            return jedis.dbSize();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void flushDb() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.flushDB()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.flushDB()));
                return;
            }
            jedis.flushDB();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void flushAll() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.flushAll()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.flushAll()));
                return;
            }
            jedis.flushAll();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void bgSave() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.bgsave()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.bgsave()));
                return;
            }
            jedis.bgsave();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public void bgReWriteAof() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.bgrewriteaof()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.bgrewriteaof()));
                return;
            }
            jedis.bgrewriteaof();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /**
     * @deprecated As of 1.3, use {@link #bgReWriteAof}.
     */
    @Deprecated
    public void bgWriteAof() {
        bgReWriteAof();
    }

    public void save() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.save()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.save()));
                return;
            }
            jedis.save();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<String> getConfig(String param) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.configGet(param)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.configGet(param)));
                return null;
            }
            return jedis.configGet(param);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Properties info() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.info(), JedisConverters.stringToProps()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.info(), JedisConverters.stringToProps()));
                return null;
            }
            return JedisConverters.toProperties(jedis.info());
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Properties info(String section) {
        if (isPipelined()) {
            throw new UnsupportedOperationException();
        }
        if (isQueueing()) {
            throw new UnsupportedOperationException();
        }
        try {
            return JedisConverters.toProperties(jedis.info(section));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long lastSave() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lastsave()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lastsave()));
                return null;
            }
            return jedis.lastsave();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void setConfig(String param, String value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.configSet(param, value)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.configSet(param, value)));
                return;
            }
            jedis.configSet(param, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void resetConfigStats() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.configResetStat()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.configResetStat()));
                return;
            }
            jedis.configResetStat();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void shutdown() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.shutdown()));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.shutdown()));
                return;
            }
            jedis.shutdown();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
    @Override
    public void shutdown(ShutdownOption option) {

        if (option == null) {
            shutdown();
            return;
        }

        eval(String.format(SHUTDOWN_SCRIPT, option.name()).getBytes(), ReturnType.STATUS, 0);
    }

    public byte[] echo(byte[] message) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.echo(message)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.echo(message)));
                return null;
            }
            return jedis.echo(message);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public String ping() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ping()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.ping()));
                return null;
            }
            return jedis.ping();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long del(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.del(keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.del(keys)));
                return null;
            }
            return jedis.del(keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void discard() {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.discard()));
                return;
            }
            transaction.discard();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        } finally {
            txResults.clear();
            transaction = null;
        }
    }

    public List<Object> exec() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.exec(), new TransactionResultConverter<Response<?>>(
                        new LinkedList<FutureResult<Response<?>>>(txResults), JedisConverters.exceptionConverter())));
                return null;
            }

            if (transaction == null) {
                throw new InvalidDataAccessApiUsageException("No ongoing transaction. Did you forget to call multi?");
            }
            List<Object> results = transaction.exec();
            return convertPipelineAndTxResults ? new TransactionResultConverter<Response<?>>(txResults,
                    JedisConverters.exceptionConverter()).convert(results) : results;
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        } finally {
            txResults.clear();
            transaction = null;
        }
    }

    public Boolean exists(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.exists(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.exists(key)));
                return null;
            }
            return jedis.exists(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean expire(byte[] key, long seconds) {

		/*
		 *  @see DATAREDIS-286 to avoid overflow in Jedis
		 *  
		 *  TODO Remove this workaround when we upgrade to a Jedis version that contains a 
		 *  fix for: https://github.com/xetorthio/jedis/pull/575
		 */
        if (seconds > Integer.MAX_VALUE) {

            return pExpireAt(key, time() + TimeUnit.SECONDS.toMillis(seconds));
        }

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.expire(key, (int) seconds), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.expire(key, (int) seconds), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.expire(key, (int) seconds));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean expireAt(byte[] key, long unixTime) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.expireAt(key, unixTime), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.expireAt(key, unixTime), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.expireAt(key, unixTime));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> keys(byte[] pattern) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.keys(pattern)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.keys(pattern)));
                return null;
            }
            return (jedis.keys(pattern));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void multi() {
        if (isQueueing()) {
            return;
        }
        try {
            if (isPipelined()) {
                pipeline.multi();
                return;
            }
            this.transaction = jedis.multi();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean persist(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.persist(key), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.persist(key), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.persist(key));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean move(byte[] key, int dbIndex) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.move(key, dbIndex), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.move(key, dbIndex), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.move(key, dbIndex));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] randomKey() {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.randomKeyBinary()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.randomKeyBinary()));
                return null;
            }
            return jedis.randomBinaryKey();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void rename(byte[] oldName, byte[] newName) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.rename(oldName, newName)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.rename(oldName, newName)));
                return;
            }
            jedis.rename(oldName, newName);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean renameNX(byte[] oldName, byte[] newName) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.renamenx(oldName, newName), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.renamenx(oldName, newName), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.renamenx(oldName, newName));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void select(int dbIndex) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.select(dbIndex)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.select(dbIndex)));
                return;
            }
            jedis.select(dbIndex);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long ttl(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.ttl(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.ttl(key)));
                return null;
            }
            return jedis.ttl(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean pExpire(byte[] key, long millis) {

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pexpire(key, millis), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.pexpire(key, millis), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.pexpire(key, millis));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pexpireAt(key, unixTimeInMillis), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.pexpireAt(key, unixTimeInMillis), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.pexpireAt(key, unixTimeInMillis));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long pTtl(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.pttl(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.pttl(key)));
                return null;
            }
            return jedis.pttl(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] dump(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.dump(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.dump(key)));
                return null;
            }
            return jedis.dump(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {

        if (ttlInMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("TtlInMillis must be less than Integer.MAX_VALUE for restore in Jedis.");
        }
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.restore(key, (int) ttlInMillis, serializedValue)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.restore(key, (int) ttlInMillis, serializedValue)));
                return;
            }
            jedis.restore(key, (int) ttlInMillis, serializedValue);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public DataType type(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.type(key), JedisConverters.stringToDataType()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.type(key), JedisConverters.stringToDataType()));
                return null;
            }
            return JedisConverters.toDataType(jedis.type(key));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void unwatch() {
        try {
            jedis.unwatch();
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void watch(byte[]... keys) {
        if (isQueueing()) {
            throw new UnsupportedOperationException();
        }
        try {
            for (byte[] key : keys) {
                if (isPipelined()) {
                    pipeline(new JedisStatusResult(pipeline.watch(key)));
                } else {
                    jedis.watch(key);
                }
            }
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // String commands
    //

    public byte[] get(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.get(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.get(key)));
                return null;
            }

            return jedis.get(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void set(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.set(key, value)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.set(key, value)));
                return;
            }
            jedis.set(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[], org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOption)
	 */
    @Override
    public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

        if (expiration == null || expiration.isPersistent()) {

            if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {
                set(key, value);
            } else {

                try {

                    byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);

                    if (isPipelined()) {

                        pipeline(new JedisStatusResult(pipeline.set(key, value, nxxx)));
                        return;
                    }
                    if (isQueueing()) {

                        transaction(new JedisStatusResult(transaction.set(key, value, nxxx)));
                        return;
                    }

                    jedis.set(key, value, nxxx);
                } catch (Exception ex) {
                    throw convertJedisAccessException(ex);
                }
            }

        } else {

            if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {

                if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
                    pSetEx(key, expiration.getExpirationTime(), value);
                } else {
                    setEx(key, expiration.getExpirationTime(), value);
                }
            } else {

                byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);
                byte[] expx = JedisConverters.toSetCommandExPxArgument(expiration);

                try {
                    if (isPipelined()) {

                        if (expiration.getExpirationTime() > Integer.MAX_VALUE) {

                            throw new IllegalArgumentException(
                                    "Expiration.expirationTime must be less than Integer.MAX_VALUE for pipeline in Jedis.");
                        }

                        pipeline(new JedisStatusResult(pipeline.set(key, value, nxxx, expx, (int) expiration.getExpirationTime())));
                        return;
                    }
                    if (isQueueing()) {

                        if (expiration.getExpirationTime() > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException(
                                    "Expiration.expirationTime must be less than Integer.MAX_VALUE for transactions in Jedis.");
                        }

                        transaction(new JedisStatusResult(transaction.set(key, value, nxxx, expx,
                                (int) expiration.getExpirationTime())));
                        return;
                    }

                    jedis.set(key, value, nxxx, expx, expiration.getExpirationTime());

                } catch (Exception ex) {
                    throw convertJedisAccessException(ex);
                }
            }
        }
    }

    public byte[] getSet(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.getSet(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.getSet(key, value)));
                return null;
            }
            return jedis.getSet(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long append(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.append(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.append(key, value)));
                return null;
            }
            return jedis.append(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> mGet(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.mget(keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.mget(keys)));
                return null;
            }
            return jedis.mget(keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void mSet(Map<byte[], byte[]> tuples) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.mset(JedisConverters.toByteArrays(tuples))));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.mset(JedisConverters.toByteArrays(tuples))));
                return;
            }
            jedis.mset(JedisConverters.toByteArrays(tuples));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean mSetNX(Map<byte[], byte[]> tuples) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.msetnx(JedisConverters.toByteArrays(tuples)), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.msetnx(JedisConverters.toByteArrays(tuples)),
                        JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.msetnx(JedisConverters.toByteArrays(tuples)));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void setEx(byte[] key, long time, byte[] value) {

        if (time > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time must be less than Integer.MAX_VALUE for setEx in Jedis.");
        }

        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.setex(key, (int) time, value)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.setex(key, (int) time, value)));
                return;
            }
            jedis.setex(key, (int) time, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
    @Override
    public void pSetEx(byte[] key, long milliseconds, byte[] value) {

        try {
            if (isPipelined()) {
                doPipelined(pipeline.psetex(key, milliseconds, value));
                return;
            }
            if (isQueueing()) {
                doQueued(transaction.psetex(key, milliseconds, value));
                return;
            }
            jedis.psetex(key, milliseconds, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean setNX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.setnx(key, value), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.setnx(key, value), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.setnx(key, value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] getRange(byte[] key, long start, long end) {

        if (start > Integer.MAX_VALUE || end > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Start and end must be less than Integer.MAX_VALUE for getRange in Jedis.");
        }

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.substr(key, (int) start, (int) end), JedisConverters.stringToBytes()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.substr(key, (int) start, (int) end), JedisConverters.stringToBytes()));
                return null;
            }
            return jedis.substr(key, (int) start, (int) end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long decr(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.decr(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.decr(key)));
                return null;
            }
            return jedis.decr(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long decrBy(byte[] key, long value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.decrBy(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.decrBy(key, value)));
                return null;
            }
            return jedis.decrBy(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long incr(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incr(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.incr(key)));
                return null;
            }
            return jedis.incr(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long incrBy(byte[] key, long value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incrBy(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.incrBy(key, value)));
                return null;
            }
            return jedis.incrBy(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Double incrBy(byte[] key, double value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.incrByFloat(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.incrByFloat(key, value)));
                return null;
            }
            return jedis.incrByFloat(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean getBit(byte[] key, long offset) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.getbit(key, offset)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.getbit(key, offset)));
                return null;
            }
            // compatibility check for Jedis 2.0.0
            Object getBit = jedis.getbit(key, offset);
            // Jedis 2.0
            if (getBit instanceof Long) {
                return (((Long) getBit) == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
            // Jedis 2.1
            return ((Boolean) getBit);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean setBit(byte[] key, long offset, boolean value) {
        try {
            if (isPipelined()) {

                pipeline(new JedisResult(pipeline.setbit(key, offset, JedisConverters.toBit(value))));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.setbit(key, offset, JedisConverters.toBit(value))));
                return null;
            }
            return jedis.setbit(key, offset, JedisConverters.toBit(value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void setRange(byte[] key, byte[] value, long start) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.setrange(key, start, value)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.setrange(key, start, value)));
                return;
            }
            jedis.setrange(key, start, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long strLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.strlen(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.strlen(key)));
                return null;
            }
            return jedis.strlen(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long bitCount(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.bitcount(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.bitcount(key)));
                return null;
            }
            return jedis.bitcount(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long bitCount(byte[] key, long begin, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.bitcount(key, begin, end)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.bitcount(key, begin, end)));
                return null;
            }
            return jedis.bitcount(key, begin, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        if (op == BitOperation.NOT && keys.length > 1) {
            throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
        }
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.bitop(JedisConverters.toBitOp(op), destination, keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.bitop(JedisConverters.toBitOp(op), destination, keys)));
                return null;
            }
            return jedis.bitop(JedisConverters.toBitOp(op), destination, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // List commands
    //

    public Long lPush(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpush(key, values)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lpush(key, values)));
                return null;
            }
            return jedis.lpush(key, values);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long rPush(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpush(key, values)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.rpush(key, values)));
                return null;
            }
            return jedis.rpush(key, values);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.blpop(bXPopArgs(timeout, keys))));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.blpop(bXPopArgs(timeout, keys))));
                return null;
            }
            return jedis.blpop(timeout, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.brpop(bXPopArgs(timeout, keys))));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.brpop(bXPopArgs(timeout, keys))));
                return null;
            }
            return jedis.brpop(timeout, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] lIndex(byte[] key, long index) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lindex(key, index)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lindex(key, index)));
                return null;
            }
            return jedis.lindex(key, index);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.linsert(key, JedisConverters.toListPosition(where), pivot, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.linsert(key, JedisConverters.toListPosition(where), pivot, value)));
                return null;
            }
            return jedis.linsert(key, JedisConverters.toListPosition(where), pivot, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long lLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.llen(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.llen(key)));
                return null;
            }
            return jedis.llen(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] lPop(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpop(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lpop(key)));
                return null;
            }
            return jedis.lpop(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> lRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lrange(key, start, end)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lrange(key, start, end)));
                return null;
            }
            return jedis.lrange(key, start, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long lRem(byte[] key, long count, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lrem(key, count, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lrem(key, count, value)));
                return null;
            }
            return jedis.lrem(key, count, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void lSet(byte[] key, long index, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.lset(key, index, value)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.lset(key, index, value)));
                return;
            }
            jedis.lset(key, index, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void lTrim(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.ltrim(key, start, end)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.ltrim(key, start, end)));
                return;
            }
            jedis.ltrim(key, start, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] rPop(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpop(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.rpop(key)));
                return null;
            }
            return jedis.rpop(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpoplpush(srcKey, dstKey)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.rpoplpush(srcKey, dstKey)));
                return null;
            }
            return jedis.rpoplpush(srcKey, dstKey);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.brpoplpush(srcKey, dstKey, timeout)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.brpoplpush(srcKey, dstKey, timeout)));
                return null;
            }
            return jedis.brpoplpush(srcKey, dstKey, timeout);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long lPushX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.lpushx(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.lpushx(key, value)));
                return null;
            }
            return jedis.lpushx(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long rPushX(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.rpushx(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.rpushx(key, value)));
                return null;
            }
            return jedis.rpushx(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // Set commands
    //

    public Long sAdd(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sadd(key, values)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sadd(key, values)));
                return null;
            }
            return jedis.sadd(key, values);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sCard(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.scard(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.scard(key)));
                return null;
            }
            return jedis.scard(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> sDiff(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sdiff(keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sdiff(keys)));
                return null;
            }
            return jedis.sdiff(keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sdiffstore(destKey, keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sdiffstore(destKey, keys)));
                return null;
            }
            return jedis.sdiffstore(destKey, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> sInter(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sinter(keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sinter(keys)));
                return null;
            }
            return jedis.sinter(keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sInterStore(byte[] destKey, byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sinterstore(destKey, keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sinterstore(destKey, keys)));
                return null;
            }
            return jedis.sinterstore(destKey, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean sIsMember(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sismember(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sismember(key, value)));
                return null;
            }
            return jedis.sismember(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> sMembers(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.smembers(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.smembers(key)));
                return null;
            }
            return jedis.smembers(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.smove(srcKey, destKey, value), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.smove(srcKey, destKey, value), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.smove(srcKey, destKey, value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] sPop(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.spop(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.spop(key)));
                return null;
            }
            return jedis.spop(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] sRandMember(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srandmember(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.srandmember(key)));
                return null;
            }
            return jedis.srandmember(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> sRandMember(byte[] key, long count) {

        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Count must be less than Integer.MAX_VALUE for sRandMember in Jedis.");
        }

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srandmember(key, (int) count)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.srandmember(key, (int) count)));
                return null;
            }
            return jedis.srandmember(key, (int) count);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sRem(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.srem(key, values)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.srem(key, values)));
                return null;
            }
            return jedis.srem(key, values);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> sUnion(byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sunion(keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sunion(keys)));
                return null;
            }
            return jedis.sunion(keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.sunionstore(destKey, keys)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.sunionstore(destKey, keys)));
                return null;
            }
            return jedis.sunionstore(destKey, keys);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // ZSet commands
    //

    public Boolean zAdd(byte[] key, double score, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zadd(key, score, value), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zadd(key, score, value), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.zadd(key, score, value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zAdd(byte[] key, Set<Tuple> tuples) {
        if (isPipelined() || isQueueing()) {
            throw new UnsupportedOperationException("zAdd of multiple fields not supported " + "in pipeline or transaction");
        }
        Map<byte[], Double> args = zAddArgs(tuples);
        try {
            return jedis.zadd(key, args);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zCard(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zcard(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zcard(key)));
                return null;
            }
            return jedis.zcard(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zCount(byte[] key, double min, double max) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zcount(key, min, max)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zcount(key, min, max)));
                return null;
            }
            return jedis.zcount(key, min, max);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Long zCount(byte[] key, Range range) {

        if (isPipelined() || isQueueing()) {
            throw new UnsupportedOperationException(
                    "ZCOUNT not implemented in jedis for binary protocol on transaction and pipeline");
        }

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        return jedis.zcount(key, min, max);
    }

    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zincrby(key, increment, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zincrby(key, increment, value)));
                return null;
            }
            return jedis.zincrby(key, increment, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        try {
            ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zinterstore(destKey, zparams, sets)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zinterstore(destKey, zparams, sets)));
                return null;
            }
            return jedis.zinterstore(destKey, zparams, sets);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zInterStore(byte[] destKey, byte[]... sets) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zinterstore(destKey, sets)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zinterstore(destKey, sets)));
                return null;
            }
            return jedis.zinterstore(destKey, sets);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> zRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrange(key, start, end)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrange(key, start, end)));
                return null;
            }
            return jedis.zrange(key, start, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrangeWithScores(key, start, end), JedisConverters.tupleSetToTupleSet()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrangeWithScores(key, start, end), JedisConverters.tupleSetToTupleSet()));
                return null;
            }
            return JedisConverters.toTupleSet(jedis.zrangeWithScores(key, start, end));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[])
	 */
    public Set<byte[]> zRangeByLex(byte[] key) {
        return zRangeByLex(key, Range.unbounded());
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    public Set<byte[]> zRangeByLex(byte[] key, Range range) {
        return zRangeByLex(key, range, null);
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
    public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

        Assert.notNull(range, "Range cannot be null for ZRANGEBYLEX.");

        byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

        try {
            if (isPipelined()) {
                if (limit != null) {
                    pipeline(new JedisResult(pipeline.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
                } else {
                    pipeline(new JedisResult(pipeline.zrangeByLex(key, min, max)));
                }
                return null;
            }

            if (isQueueing()) {
                if (limit != null) {
                    transaction(new JedisResult(transaction.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
                } else {
                    transaction(new JedisResult(transaction.zrangeByLex(key, min, max)));
                }
                return null;
            }

            if (limit != null) {
                return jedis.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount());
            }
            return jedis.zrangeByLex(key, min, max);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double)
	 */
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
        return zRangeByScore(key, new Range().gte(min).lte(max));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range) {
        return zRangeByScore(key, range, null);
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
    @Override
    public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

        Assert.notNull(range, "Range cannot be null for ZRANGEBYSCORE.");

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        try {
            if (isPipelined()) {
                if (limit != null) {
                    pipeline(new JedisResult(pipeline.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
                } else {
                    pipeline(new JedisResult(pipeline.zrangeByScore(key, min, max)));
                }
                return null;
            }

            if (isQueueing()) {
                if (limit != null) {
                    transaction(new JedisResult(transaction.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
                } else {
                    transaction(new JedisResult(transaction.zrangeByScore(key, min, max)));
                }
                return null;
            }

            if (limit != null) {
                return jedis.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount());
            }
            return jedis.zrangeByScore(key, min, max);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double)
	 */
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
        return zRangeByScoreWithScores(key, new Range().gte(min).lte(max));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
        return zRangeByScoreWithScores(key, range, null);
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

        Assert.notNull(range, "Range cannot be null for ZRANGEBYSCOREWITHSCORES.");

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        try {
            if (isPipelined()) {
                if (limit != null) {
                    pipeline(new JedisResult(
                            pipeline.zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()),
                            JedisConverters.tupleSetToTupleSet()));
                } else {
                    pipeline(new JedisResult(pipeline.zrangeByScoreWithScores(key, min, max),
                            JedisConverters.tupleSetToTupleSet()));
                }
                return null;
            }

            if (isQueueing()) {
                if (limit != null) {
                    transaction(new JedisResult(transaction.zrangeByScoreWithScores(key, min, max, limit.getOffset(),
                            limit.getCount()), JedisConverters.tupleSetToTupleSet()));
                } else {
                    transaction(new JedisResult(transaction.zrangeByScoreWithScores(key, min, max),
                            JedisConverters.tupleSetToTupleSet()));
                }
                return null;
            }

            if (limit != null) {
                return JedisConverters.toTupleSet(jedis.zrangeByScoreWithScores(key, min, max, limit.getOffset(),
                        limit.getCount()));
            }
            return JedisConverters.toTupleSet(jedis.zrangeByScoreWithScores(key, min, max));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrangeWithScores(key, start, end), JedisConverters.tupleSetToTupleSet()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrevrangeWithScores(key, start, end),
                        JedisConverters.tupleSetToTupleSet()));
                return null;
            }
            return JedisConverters.toTupleSet(jedis.zrevrangeWithScores(key, start, end));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }

    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
	 */
    @Override
    public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {

        return zRangeByScore(key, new Range().gte(min).lte(max),
                new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double, long, long)
	 */
    @Override
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

        return zRangeByScoreWithScores(key, new Range().gte(min).lte(max),
                new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double, long, long)
	 */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

        return zRevRangeByScore(key, new Range().gte(min).lte(max), new Limit().offset(Long.valueOf(offset).intValue())
                .count(Long.valueOf(count).intValue()));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double)
	 */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
        return zRevRangeByScore(key, new Range().gte(min).lte(max));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
        return zRevRangeByScore(key, range, null);
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
    @Override
    public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

        Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCORE.");

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        try {
            if (isPipelined()) {
                if (limit != null) {
                    pipeline(new JedisResult(pipeline.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount())));
                } else {
                    pipeline(new JedisResult(pipeline.zrevrangeByScore(key, max, min)));
                }
                return null;
            }

            if (isQueueing()) {
                if (limit != null) {
                    transaction(new JedisResult(transaction.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount())));
                } else {
                    transaction(new JedisResult(transaction.zrevrangeByScore(key, max, min)));
                }
                return null;
            }

            if (limit != null) {
                return jedis.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount());
            }
            return jedis.zrevrangeByScore(key, max, min);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double, long, long)
	 */
    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

        return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max),
                new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
        return zRevRangeByScoreWithScores(key, range, null);
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
    @Override
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

        Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCOREWITHSCORES.");

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        try {
            if (isPipelined()) {
                if (limit != null) {
                    pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(),
                            limit.getCount()), JedisConverters.tupleSetToTupleSet()));
                } else {
                    pipeline(new JedisResult(pipeline.zrevrangeByScoreWithScores(key, max, min),
                            JedisConverters.tupleSetToTupleSet()));
                }
                return null;
            }

            if (isQueueing()) {
                if (limit != null) {
                    transaction(new JedisResult(transaction.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(),
                            limit.getCount()), JedisConverters.tupleSetToTupleSet()));
                } else {
                    transaction(new JedisResult(transaction.zrevrangeByScoreWithScores(key, max, min),
                            JedisConverters.tupleSetToTupleSet()));
                }
                return null;
            }

            if (limit != null) {
                return JedisConverters.toTupleSet(jedis.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(),
                        limit.getCount()));
            }
            return JedisConverters.toTupleSet(jedis.zrevrangeByScoreWithScores(key, max, min));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double)
	 */
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
        return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max), null);
    }

    public Long zRank(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrank(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrank(key, value)));
                return null;
            }
            return jedis.zrank(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zRem(byte[] key, byte[]... values) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrem(key, values)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrem(key, values)));
                return null;
            }
            return jedis.zrem(key, values);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zRemRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zremrangeByRank(key, start, end)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zremrangeByRank(key, start, end)));
                return null;
            }
            return jedis.zremrangeByRank(key, start, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], double, double)
	 */
    @Override
    public Long zRemRangeByScore(byte[] key, double min, double max) {
        return zRemRangeByScore(key, new Range().gte(min).lte(max));
    }

    /*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
    @Override
    public Long zRemRangeByScore(byte[] key, Range range) {

        Assert.notNull(range, "Range cannot be null for ZREMRANGEBYSCORE.");

        byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
        byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zremrangeByScore(key, min, max)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zremrangeByScore(key, min, max)));
                return null;
            }
            return jedis.zremrangeByScore(key, min, max);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrange(key, start, end)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrevrange(key, start, end)));
                return null;
            }
            return jedis.zrevrange(key, start, end);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zRevRank(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zrevrank(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zrevrank(key, value)));
                return null;
            }
            return jedis.zrevrank(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Double zScore(byte[] key, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zscore(key, value)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zscore(key, value)));
                return null;
            }
            return jedis.zscore(key, value);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
        try {
            ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zunionstore(destKey, zparams, sets)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zunionstore(destKey, zparams, sets)));
                return null;
            }
            return jedis.zunionstore(destKey, zparams, sets);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.zunionstore(destKey, sets)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.zunionstore(destKey, sets)));
                return null;
            }
            return jedis.zunionstore(destKey, sets);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // Hash commands
    //

    public Boolean hSet(byte[] key, byte[] field, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hset(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hset(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.hset(key, field, value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hsetnx(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hsetnx(key, field, value), JedisConverters.longToBoolean()));
                return null;
            }
            return JedisConverters.toBoolean(jedis.hsetnx(key, field, value));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long hDel(byte[] key, byte[]... fields) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hdel(key, fields)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hdel(key, fields)));
                return null;
            }
            return jedis.hdel(key, fields);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Boolean hExists(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hexists(key, field)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hexists(key, field)));
                return null;
            }
            return jedis.hexists(key, field);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public byte[] hGet(byte[] key, byte[] field) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hget(key, field)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hget(key, field)));
                return null;
            }
            return jedis.hget(key, field);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Map<byte[], byte[]> hGetAll(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hgetAll(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hgetAll(key)));
                return null;
            }
            return jedis.hgetAll(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long hIncrBy(byte[] key, byte[] field, long delta) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hincrBy(key, field, delta)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hincrBy(key, field, delta)));
                return null;
            }
            return jedis.hincrBy(key, field, delta);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Double hIncrBy(byte[] key, byte[] field, double delta) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hincrByFloat(key, field, delta)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hincrByFloat(key, field, delta)));
                return null;
            }
            return jedis.hincrByFloat(key, field, delta);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Set<byte[]> hKeys(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hkeys(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hkeys(key)));
                return null;
            }
            return jedis.hkeys(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Long hLen(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hlen(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hlen(key)));
                return null;
            }
            return jedis.hlen(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> hMGet(byte[] key, byte[]... fields) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hmget(key, fields)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hmget(key, fields)));
                return null;
            }
            return jedis.hmget(key, fields);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
        try {
            if (isPipelined()) {
                pipeline(new JedisStatusResult(pipeline.hmset(key, tuple)));
                return;
            }
            if (isQueueing()) {
                transaction(new JedisStatusResult(transaction.hmset(key, tuple)));
                return;
            }
            jedis.hmset(key, tuple);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public List<byte[]> hVals(byte[] key) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.hvals(key)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.hvals(key)));
                return null;
            }
            return jedis.hvals(key);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // Pub/Sub functionality
    //

    public Long publish(byte[] channel, byte[] message) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.publish(channel, message)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.publish(channel, message)));
                return null;
            }
            return jedis.publish(channel, message);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public boolean isSubscribed() {
        return (subscription != null && subscription.isAlive());
    }

    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        if (isSubscribed()) {
            throw new RedisSubscribedConnectionException(
                    "Connection already subscribed; use the connection Subscription to cancel or add new channels");
        }
        if (isQueueing()) {
            throw new UnsupportedOperationException();
        }
        if (isPipelined()) {
            throw new UnsupportedOperationException();
        }

        try {
            BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

            subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
            jedis.psubscribe(jedisPubSub, patterns);

        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    public void subscribe(MessageListener listener, byte[]... channels) {
        if (isSubscribed()) {
            throw new RedisSubscribedConnectionException(
                    "Connection already subscribed; use the connection Subscription to cancel or add new channels");
        }

        if (isQueueing()) {
            throw new UnsupportedOperationException();
        }
        if (isPipelined()) {
            throw new UnsupportedOperationException();
        }

        try {
            BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);

            subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
            jedis.subscribe(jedisPubSub, channels);

        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    //
    // Geo commands
    //
    @Override
    public Long geoAdd(byte[] key, double longitude, double latitude, byte[] member) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, longitude, latitude, member)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geoadd(key, longitude, latitude, member)));
                return null;
            }

            return jedis.geoadd(key, longitude, latitude, member);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public Long geoAdd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap){
        try {
            Map<byte[], redis.clients.jedis.GeoCoordinate> redisGeoCoordinateMap = new HashMap<byte[], redis.clients.jedis.GeoCoordinate>();
            for(byte[] mapKey : memberCoordinateMap.keySet()){
                redisGeoCoordinateMap.put(mapKey, JedisConverters.toGeoCoordinate(memberCoordinateMap.get(mapKey)));
            }

            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geoadd(key, redisGeoCoordinateMap)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geoadd(key, redisGeoCoordinateMap)));
                return null;
            }

            return jedis.geoadd(key, redisGeoCoordinateMap);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }


    @Override
    public Double geoDist(byte[] key, byte[] member1, byte[] member2) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geodist(key, member1, member2)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geodist(key, member1, member2)));
                return null;
            }

            return jedis.geodist(key, member1, member2);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public Double geoDist(byte[] key, byte[] member1, byte[] member2, org.springframework.data.redis.core.GeoUnit unit) {
        GeoUnit geoUnit = JedisConverters.toGeoUnit(unit);
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geodist(key, member1, member2, geoUnit)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geodist(key, member1, member2, geoUnit)));
                return null;
            }

            return jedis.geodist(key, member1, member2, geoUnit);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<byte[]> geoHash(byte[] key, byte[]... members) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geohash(key, members)));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geohash(key, members)));
                return null;
            }

            return jedis.geohash(key, members);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<GeoCoordinate> geoPos(byte[] key, byte[]... members) {
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.geopos(key, members), JedisConverters.geoCoordinateListToGeoCoordinateList()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.geopos(key, members), JedisConverters.geoCoordinateListToGeoCoordinateList()));
                return null;
            }

            return JedisConverters.geoCoordinateListToGeoCoordinateList().convert(jedis.geopos(key, members));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
        double radius, org.springframework.data.redis.core.GeoUnit unit) {
        GeoUnit geoUnit = JedisConverters.toGeoUnit(unit);
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadius(key, longitude, latitude, radius, geoUnit), JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.georadius(key, longitude, latitude, radius, geoUnit), JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }

            return JedisConverters.geoRadiusResponseGeoRadiusResponseList().convert(jedis.georadius(key, longitude, latitude, radius, geoUnit));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude,
        double radius, org.springframework.data.redis.core.GeoUnit unit, GeoRadiusParam param) {
        GeoUnit geoUnit = JedisConverters.toGeoUnit(unit);
        redis.clients.jedis.params.geo.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(param);
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadius(key, longitude, latitude, radius, geoUnit, geoRadiusParam),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.georadius(key, longitude, latitude, radius, geoUnit, geoRadiusParam),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }

            return JedisConverters.geoRadiusResponseGeoRadiusResponseList().
                convert(jedis.georadius(key, longitude, latitude, radius, geoUnit, geoRadiusParam));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
        org.springframework.data.redis.core.GeoUnit unit) {
        GeoUnit geoUnit = JedisConverters.toGeoUnit(unit);
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadiusByMember(key, member, radius, geoUnit),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.georadiusByMember(key, member, radius, geoUnit),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }

            return JedisConverters.geoRadiusResponseGeoRadiusResponseList().convert(jedis.georadiusByMember(key, member, radius, geoUnit));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius,
        org.springframework.data.redis.core.GeoUnit unit, GeoRadiusParam param) {
        GeoUnit geoUnit = JedisConverters.toGeoUnit(unit);
        redis.clients.jedis.params.geo.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(param);
        try {
            if (isPipelined()) {
                pipeline(new JedisResult(pipeline.georadiusByMember(key, member, radius, geoUnit, geoRadiusParam),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }
            if (isQueueing()) {
                transaction(new JedisResult(transaction.georadiusByMember(key, member, radius, geoUnit, geoRadiusParam),
                    JedisConverters.geoRadiusResponseGeoRadiusResponseList()));
                return null;
            }
            return JedisConverters.geoRadiusResponseGeoRadiusResponseList().
                convert(jedis.georadiusByMember(key, member, radius, geoUnit, geoRadiusParam));
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }


	//
	// Scripting commands
	//

	public void scriptFlush() {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			jedis.scriptFlush();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	public void scriptKill() {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			jedis.scriptKill();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	public String scriptLoad(byte[] script) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			return JedisConverters.toString(jedis.scriptLoad(script));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	public List<Boolean> scriptExists(String... scriptSha1) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			return jedis.scriptExists(scriptSha1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			return (T) new JedisScriptReturnConverter(returnType).convert(jedis.eval(script,
					JedisConverters.toBytes(numKeys), keysAndArgs));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return evalSha(JedisConverters.toBytes(scriptSha1), returnType, numKeys, keysAndArgs);
	}

	@SuppressWarnings("unchecked")
	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
		try {
			return (T) new JedisScriptReturnConverter(returnType).convert(jedis.evalsha(scriptSha1, numKeys, keysAndArgs));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {

		List<String> serverTimeInformation = this.jedis.time();

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid nr of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(serverTimeInformation.get(0), serverTimeInformation.get(1));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#killClient(byte[])
	 */
	@Override
	public void killClient(String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'.");

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'CLIENT KILL' is not supported in transaction / pipline mode.");
		}

		try {
			this.jedis.clientKill(String.format("%s:%s", host, port));
		} catch (Exception e) {
			throw convertJedisAccessException(e);
		}
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {

		Assert.hasText(host, "Host must not be null for 'SLAVEOF' command.");
		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'SLAVEOF' cannot be called in pipline / transaction mode.");
		}
		try {
			this.jedis.slaveof(host, port);
		} catch (Exception e) {
			throw convertJedisAccessException(e);
		}
	}

	/*
	* @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(java.lang.String)
	*/
	@Override
	public void setClientName(byte[] name) {

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException("'CLIENT SETNAME' is not suppored in transacton / pipeline mode.");
		}

		jedis.clientSetname(name);
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException();
		}

		return jedis.clientGetname();
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'CLIENT LIST' is not supported in in pipeline / multi mode.");
		}
		return JedisConverters.toListOfRedisClientInformation(this.jedis.clientList());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {

		if (isQueueing() || isPipelined()) {
			throw new UnsupportedOperationException("'SLAVEOF' cannot be called in pipline / transaction mode.");
		}
		try {
			this.jedis.slaveofNoOne();
		} catch (Exception e) {
			throw convertJedisAccessException(e);
		}
	}

	/**
	 * @since 1.4
	 * @return
	 */
	public Cursor<byte[]> scan() {
		return scan(ScanOptions.NONE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	public Cursor<byte[]> scan(ScanOptions options) {
		return scan(0, options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'SCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);
				redis.clients.jedis.ScanResult<String> result = jedis.scan(Long.toString(cursorId), params);
				return new ScanIteration<byte[]>(Long.valueOf(result.getStringCursor()), JedisConverters.stringListToByteList()
						.convert(result.getResult()));
			}

		}.open();

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return zScan(key, 0L, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Tuple> zScan(byte[] key, Long cursorId, ScanOptions options) {

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<redis.clients.jedis.Tuple> result = jedis.zscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<RedisZSetCommands.Tuple>(Long.valueOf(result.getStringCursor()), JedisConverters
						.tuplesToTuples().convert(result.getResult()));
			}

		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		return sScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> sScan(byte[] key, long cursorId, ScanOptions options) {

		return new KeyBoundCursor<byte[]>(key, cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				redis.clients.jedis.ScanResult<byte[]> result = jedis.sscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<byte[]>(Long.valueOf(result.getStringCursor()), result.getResult());
			}
		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		return hScan(key, 0, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, long cursorId, ScanOptions options) {

		return new KeyBoundCursor<Map.Entry<byte[], byte[]>>(key, cursorId, options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<Entry<byte[], byte[]>> result = jedis.hscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<Map.Entry<byte[], byte[]>>(Long.valueOf(result.getStringCursor()), result.getResult());
			}
		}.open();
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type. If false, results of
	 * {@link #closePipeline()} and {@link #exec()} will be of the type returned by the Jedis driver
	 *
	 * @param convertPipelineAndTxResults Whether or not to convert pipeline and tx results
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	private byte[][] bXPopArgs(int timeout, byte[]... keys) {
		final List<byte[]> args = new ArrayList<byte[]>();
		for (final byte[] arg : keys) {
			args.add(arg);
		}
		args.add(Protocol.toByteArray(timeout));
		return args.toArray(new byte[args.size()][]);
	}

	private Map<byte[], Double> zAddArgs(Set<Tuple> tuples) {

		Map<byte[], Double> args = new LinkedHashMap<byte[], Double>(tuples.size(), 1);
		Set<Double> scores = new HashSet<Double>(tuples.size(), 1);

		boolean isAtLeastJedis24 = JedisVersionUtil.atLeastJedis24();

		for (Tuple tuple : tuples) {

			if (!isAtLeastJedis24) {
				if (scores.contains(tuple.getScore())) {
					throw new UnsupportedOperationException(
							"Bulk add of multiple elements with the same score is not supported. Add the elements individually.");
				}
				scores.add(tuple.getScore());
			}

			args.put(tuple.getValue(), tuple.getScore());
		}

		return args;
	}

	@Override
	protected boolean isActive(RedisNode node) {

		if (node == null) {
			return false;
		}

		Jedis temp = null;
		try {
			temp = getJedis(node);
			temp.connect();
			return temp.ping().equalsIgnoreCase("pong");
		} catch (Exception e) {
			return false;
		} finally {
			if (temp != null) {
				temp.disconnect();
				temp.close();
			}
		}
	}

	@Override
	protected JedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		return new JedisSentinelConnection(getJedis(sentinel));
	}

	protected Jedis getJedis(RedisNode node) {
		return new Jedis(node.getHost(), node.getPort());
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

		try {
			String keyStr = new String(key, "UTF-8");
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.zrangeByScore(keyStr, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.zrangeByScore(keyStr, min, max)));
				return null;
			}
			return JedisConverters.stringSetToByteSet().convert(jedis.zrangeByScore(keyStr, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {

			throw new IllegalArgumentException(
					"Offset and count must be less than Integer.MAX_VALUE for zRangeByScore in Jedis.");
		}

		try {
			String keyStr = new String(key, "UTF-8");
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			return JedisConverters.stringSetToByteSet().convert(
					jedis.zrangeByScore(keyStr, min, max, (int) offset, (int) count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfAdd(byte[], byte[][])
	 */
	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value.");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.pfadd(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.pfadd(key, values)));
				return null;
			}
			return jedis.pfadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key.");
		Assert.noNullElements(keys, "Keys for PFOUNT must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.pfcount(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.pfcount(keys)));
				return null;
			}
			return jedis.pfcount(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		try {
			if (isPipelined()) {
				pipeline(new JedisResult(pipeline.pfmerge(destinationKey, sourceKeys)));
				return;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.pfmerge(destinationKey, sourceKeys)));
				return;
			}
			jedis.pfmerge(destinationKey, sourceKeys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption, long)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout) {

		final int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		try {
			if (isPipelined()) {

				pipeline(new JedisResult(pipeline.migrate(JedisConverters.toBytes(target.getHost()), target.getPort(), key,
						dbIndex, timeoutToUse)));
				return;
			}
			if (isQueueing()) {
				transaction(new JedisResult(transaction.migrate(JedisConverters.toBytes(target.getHost()), target.getPort(),
						key, dbIndex, timeoutToUse)));
				return;
			}
			jedis.migrate(JedisConverters.toBytes(target.getHost()), target.getPort(), key, dbIndex, timeoutToUse);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

}
