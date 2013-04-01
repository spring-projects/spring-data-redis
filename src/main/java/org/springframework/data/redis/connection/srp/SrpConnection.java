/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.util.Assert;

import redis.Command;
import redis.client.RedisClient;
import redis.client.RedisClient.Pipeline;
import redis.client.RedisException;
import redis.reply.Reply;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/spullara/redis-protocol">spullara Redis Protocol</a> library.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public class SrpConnection implements RedisConnection {

	private final RedisClient client;
	private final BlockingQueue<SrpConnection> queue;

	private boolean isClosed = false;
	private boolean isMulti = false;
	private Pipeline pipeline;
	private PipelineTracker callback;
	private volatile SrpSubscription subscription;

	private static class PipelineTracker implements FutureCallback<Reply> {

		private final List<Object> results = Collections.synchronizedList(new ArrayList<Object>());
		private final List<ListenableFuture<? extends Reply>> futures = new ArrayList<ListenableFuture<? extends Reply>>();

		public void onSuccess(Reply result) {
			results.add(result.data());
		}

		public void onFailure(Throwable t) {
			results.add(t);
		}

		public List<Object> complete() {
			try {
				Futures.successfulAsList(futures).get();
			} catch (Exception ex) {
				// ignore
			}

			return results;
		}

		public void addCommand(ListenableFuture<? extends Reply> future) {
			futures.add(future);
			Futures.addCallback(future, this);
		}

		public void close() {
			results.clear();
			futures.clear();
		}
	}

	public SrpConnection(String host, int port, BlockingQueue<SrpConnection> queue) {
		try {
			this.client = new RedisClient(host, port);
			this.queue = queue;
		} catch (IOException e) {
			throw new RedisConnectionFailureException("Could not connect", e);
		}
	}

	protected DataAccessException convertSrpAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return SrpUtils.convertSRedisAccessException((RedisException) ex);
		}
		if (ex instanceof IOException) {
			return new RedisConnectionFailureException("Redis connection failed", (IOException) ex);
		}

		return new RedisSystemException("Unknown SRP exception", ex);
	}

	public Object execute(String command, byte[]... args) {
		Assert.hasText(command, "a valid command needs to be specified");
		try {
			String name = command.trim().toUpperCase();
			Command cmd = new Command(name.getBytes(Charsets.UTF_8), args);
			if (isPipelined()) {
				pipeline(client.pipeline(name, cmd));
				return null;
			}
			else {
				return client.execute(name, cmd);
			}
		} catch (RedisException ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public void close() throws DataAccessException {
		isClosed = true;
		queue.remove(this);

		if (subscription != null) {
			subscription.doClose();
			subscription = null;
		}

		try {
			client.close();
		} catch (IOException ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public boolean isClosed() {
		return isClosed;
	}

	public RedisClient getNativeConnection() {
		return client;
	}


	public boolean isQueueing() {
		return isMulti;
	}

	public boolean isPipelined() {
		return (pipeline != null);
	}


	public void openPipeline() {
		if (pipeline == null) {
			callback = new PipelineTracker();
			pipeline = client.pipeline();
		}
	}

	public List<Object> closePipeline() {
		if (pipeline != null) {
			pipeline = null;
			List<Object> execute = new ArrayList<Object>(callback.complete());
			callback.close();
			callback = null;
			if (execute != null && !execute.isEmpty()) {
				Exception cause = null;
				for (int i = 0; i < execute.size(); i++) {
					Object object = execute.get(i);
					if (object instanceof Exception) {
						DataAccessException dataAccessException = convertSrpAccessException((Exception) object);
						if (cause == null) {
							cause = dataAccessException;
						}
						execute.set(i, dataAccessException);
					}
				}
				if (cause != null) {
					throw new RedisPipelineException(cause, execute);
				}

				return execute;
			}
		}

		return Collections.emptyList();
	}


	public List<byte[]> sort(byte[] key, SortParameters params) {

		byte[] sort = SrpUtils.sort(params);

		try {
			if (isPipelined()) {
				pipeline(pipeline.sort(key, sort, null, (Object[]) null));
				return null;
			}
			return SrpUtils.toBytesList((Reply[]) client.sort(key, sort, null, (Object[]) null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		byte[] sort = SrpUtils.sort(params, sortKey);

		try {
			if (isPipelined()) {
				pipeline(pipeline.sort(key, sort, null, (Object[]) null));
				return null;
			}
			return ((Long) client.sort(key, sort, null, (Object[]) null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.dbsize());
				return null;
			}
			return client.dbsize().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}



	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.flushdb());
				return;
			}
			client.flushdb();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.flushall());
				return;
			}
			client.flushall();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.bgsave());
				return;
			}
			client.bgsave();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void bgWriteAof() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.bgrewriteaof());
				return;
			}
			client.bgrewriteaof();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void save() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.save());
				return;
			}
			client.save();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.config_get(param));
				return null;
			}
			return Collections.singletonList(client.config_get(param).toString());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.info());
				return null;
			}
			return SrpUtils.info(client.info());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lastsave());
				return null;
			}
			return client.lastsave().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.config_set(param, value));
				return;
			}
			client.config_set(param, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}



	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.config_resetstat());
				return;
			}
			client.config_resetstat();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void shutdown() {
		byte[] save = "SAVE".getBytes(Charsets.UTF_8);
		try {
			if (isPipelined()) {
				pipeline(pipeline.shutdown(save, null));
				return;
			}
			client.shutdown(save, null);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.echo(message));
				return null;
			}
			return client.echo(message).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public String ping() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.ping());
				return null;
			}
			return client.ping().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long del(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.del((Object[]) keys));
				return null;
			}
			return client.del((Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void discard() {
		isMulti = false;
		try {
			if (isPipelined()) {
				// use the normal path
			}

			client.discard();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<Object> exec() {
		isMulti = false;
		try {
			Future<Boolean> exec = client.exec();
			if (!isPipelined()) {
				exec.get();
			}
			return Collections.singletonList((Object) exec);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.exists(key));
				return null;
			}
			return client.exists(key).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.expire(key, seconds));
				return null;
			}
			return client.expire(key, seconds).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.expireat(key, unixTime));
				return null;
			}
			return client.expireat(key, unixTime).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> keys(byte[] pattern) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.keys(pattern));
				return null;
			}
			return SrpUtils.toSet(client.keys(pattern).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		openPipeline();
		try {
			if (isPipelined()) {
				client.multi();
				return;
			}
			client.multi();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.persist(key));
				return null;
			}
			return client.persist(key).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean move(byte[] key, int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.move(key, dbIndex));
				return null;
			}
			return client.move(key, dbIndex).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.randomkey());
				return null;
			}
			return client.randomkey().data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void rename(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.rename(oldName, newName));
				return;
			}
			client.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.renamenx(oldName, newName));
				return null;
			}
			return (client.renamenx(oldName, newName).data() == 1);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.select(dbIndex));
			}
			client.select(dbIndex);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.ttl(key));
				return null;
			}
			return client.ttl(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public DataType type(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.type(key));
				return null;
			}
			return DataType.fromCode(client.type(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline(pipeline.unwatch());
				return;
			}
			client.unwatch();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			if (isPipelined()) {
				pipeline(pipeline.watch((Object[]) keys));
				return;
			}
			else {
				client.watch((Object[]) keys);
			}
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// String commands
	//


	public byte[] get(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.get(key));
				return null;
			}

			return client.get(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void set(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.set(key, value));
				return;
			}
			client.set(key, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}



	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.getset(key, value));
				return null;
			}
			return client.getset(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long append(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.append(key, value));
				return null;
			}
			return client.append(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> mGet(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.mget((Object[]) keys));
				return null;
			}
			return SrpUtils.toBytesList(client.mget((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.mset((Object[]) SrpUtils.convert(tuples)));
				return;
			}
			client.mset((Object[]) SrpUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.msetnx((Object[]) SrpUtils.convert(tuples)));
				return;
			}
			client.msetnx((Object[]) SrpUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.setex(key, time, value));
				return;
			}
			client.setex(key, time, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.setnx(key, value));
				return null;
			}
			return client.setnx(key, value).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] getRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.getrange(key, start, end));
				return null;
			}
			return client.getrange(key, start, end).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long decr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.decr(key));
				return null;
			}
			return client.decr(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long decrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.decrby(key, value));
				return null;
			}
			return client.decrby(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long incr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.incr(key));
				return null;
			}
			return client.incr(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long incrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.incrby(key, value));
				return null;
			}
			return client.incrby(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.getbit(key, offset));
				return null;
			}
			return (client.getbit(key, offset).data() == 1);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.setbit(key, offset, SrpUtils.asBit(value)));
				return;
			}
			client.setbit(key, offset, SrpUtils.asBit(value));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.setrange(key, start, value));
				return;
			}
			client.setrange(key, start, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.strlen(key));
				return null;
			}
			return client.strlen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// List commands
	//


	public Long lPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lpush(key, new Object[] { value }));
				return null;
			}
			return client.lpush(key, new Object[] { value }).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long rPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.rpush(key, new Object[] { value }));
				return null;
			}
			return client.rpush(key, new Object[] { value }).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		Object[] args = SrpUtils.convert(timeout, keys);

		try {
			if (isPipelined()) {
				pipeline(pipeline.blpop(args));
				return null;
			}
			return SrpUtils.toBytesList(client.blpop(args).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		Object[] args = SrpUtils.convert(timeout, keys);
		try {
			if (isPipelined()) {
				pipeline(pipeline.brpop(args));
				return null;
			}
			return SrpUtils.toBytesList(client.brpop(args).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lindex(key, index));
				return null;
			}
			return client.lindex(key, index).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.linsert(key, SrpUtils.convertPosition(where), pivot, value));
				return null;
			}
			return client.linsert(key, SrpUtils.convertPosition(where), pivot, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.llen(key));
				return null;
			}
			return client.llen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] lPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lpop(key));
				return null;
			}
			return client.lpop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lrange(key, start, end));
				return null;
			}
			return SrpUtils.toBytesList(client.lrange(key, start, end).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lrem(key, count, value));
				return null;
			}
			return client.lrem(key, count, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lset(key, index, value));
				return;
			}
			client.lset(key, index, value);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.ltrim(key, start, end));
				return;
			}
			client.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] rPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.rpop(key));
				return null;
			}
			return client.rpop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.rpoplpush(srcKey, dstKey));
				return null;
			}
			return client.rpoplpush(srcKey, dstKey).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.brpoplpush(srcKey, dstKey, timeout));
				return null;
			}
			return client.brpoplpush(srcKey, dstKey, timeout).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.lpushx(key, value));
				return null;
			}
			return client.lpushx(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.rpushx(key, value));
				return null;
			}
			return client.rpushx(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	//
	// Set commands
	//


	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sadd(key, new Object[] { value }));
				return null;
			}
			return (client.sadd(key, new Object[] { value }).data() == 1);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.scard(key));
				return null;
			}
			return client.scard(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sdiff((Object[]) keys));
				return null;
			}
			return SrpUtils.toSet(client.sdiff((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sdiffstore(destKey, (Object[]) keys));
				return null;
			}
			return client.sdiffstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sinter((Object[]) keys));
				return null;
			}
			return SrpUtils.toSet(client.sinter((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sinterstore(destKey, (Object[]) keys));
				return null;
			}
			return client.sinterstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sismember(key, value));
				return null;
			}
			return client.sismember(key, value).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.smembers(key));
				return null;
			}
			return SrpUtils.toSet(client.smembers(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.smove(srcKey, destKey, value));
				return null;
			}
			return client.smove(srcKey, destKey, value).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.spop(key));
				return null;
			}
			return client.spop(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.srandmember(key));
				return null;
			}
			return client.srandmember(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean sRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.srem(key, new Object[] { value }));
				return null;
			}
			return client.srem(key, new Object[] { value }).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sunion((Object[]) keys));
				return null;
			}
			return SrpUtils.toSet(client.sunion((Object[]) keys).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.sunionstore(destKey, (Object[]) keys));
				return null;
			}
			return client.sunionstore(destKey, (Object[]) keys).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// ZSet commands
	//


	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zadd(new Object[] { key, score, value }));
				return null;
			}
			return client.zadd(new Object[] { key, score, value }).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zcard(key));
				return null;
			}
			return client.zcard(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zcount(key, min, max));
				return null;
			}
			return client.zcount(key, min, max).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zincrby(key, increment, value));
				return null;
			}
			return SrpUtils.toDouble(client.zincrby(key, increment, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}


	public Long zInterStore(byte[] destKey, byte[]... sets) {

		Object[] args = new Object[2 + sets.length];

		args[0] = destKey;
		args[1] = sets.length;
		int i = 2;
		for (byte[] set : sets) {
			args[i++] = set;
		}

		try {
			if (isPipelined()) {
				pipeline(pipeline.zinterstore(args));
				return null;
			}
			return client.zinterstore(args).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrange(key, start, end, null));
				return null;
			}
			return SrpUtils.toSet(client.zrange(key, start, end, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrange(key, start, end, SrpUtils.WITHSCORES));
				return null;
			}
			return SrpUtils.convertTuple(client.zrange(key, start, end, SrpUtils.WITHSCORES));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrangebyscore(key, min, max, null, null));
				return null;
			}
			return SrpUtils.toSet(client.zrangebyscore(key, min, max, null, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrangebyscore(key, min, max, SrpUtils.WITHSCORES, null));
				return null;
			}
			return SrpUtils.convertTuple(client.zrangebyscore(key, min, max, SrpUtils.WITHSCORES, null));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrevrange(key, start, end, SrpUtils.WITHSCORES));
				return null;
			}
			return SrpUtils.convertTuple(client.zrevrange(key, start, end, SrpUtils.WITHSCORES));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SrpUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline(pipeline.zrangebyscore(key, min, max, null, limit));
				return null;
			}
			return SrpUtils.toSet(client.zrangebyscore(key, min, max, null, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SrpUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline(pipeline.zrangebyscore(key, min, max, SrpUtils.WITHSCORES, limit));
				return null;
			}
			return SrpUtils.convertTuple(client.zrangebyscore(key, min, max, SrpUtils.WITHSCORES, limit));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SrpUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline(pipeline.zrevrangebyscore(key, min, max, null, limit));
				return null;
			}
			return SrpUtils.toSet(client.zrevrangebyscore(key, min, max, null, limit).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrevrangebyscore(key, min, max, null, null));
				return null;
			}
			return SrpUtils.toSet(client.zrevrangebyscore(key, min, max, null, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SrpUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline(pipeline.zrevrangebyscore(key, min, max, SrpUtils.WITHSCORES, limit));
				return null;
			}
			return SrpUtils.convertTuple(client.zrevrangebyscore(key, min, max, SrpUtils.WITHSCORES, limit));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrevrangebyscore(key, min, max, SrpUtils.WITHSCORES, null));
				return null;
			}
			return SrpUtils.convertTuple(client.zrevrangebyscore(key, min, max, SrpUtils.WITHSCORES, null));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrank(key, value));
				return null;
			}
			return (Long) client.zrank(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean zRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrem(key, new Object[] { value }));
				return null;
			}
			return client.zrem(key, new Object[] { value }).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zremrangebyrank(key, start, end));
				return null;
			}
			return client.zremrangebyrank(key, start, end).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zremrangebyscore(key, min, max));
				return null;
			}
			return client.zremrangebyscore(key, min, max).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrevrange(key, start, end, null));
				return null;
			}
			return SrpUtils.toSet(client.zrevrange(key, start, end, null).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zrevrank(key, value));
				return null;
			}
			return (Long) client.zrevrank(key, value).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Double zScore(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zscore(key, value));
				return null;
			}
			return SrpUtils.toDouble(client.zscore(key, value).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}


	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.zunionstore(destKey, sets.length, (Object[]) sets));
				return null;
			}
			return client.zunionstore(destKey, sets.length, (Object[]) sets).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	//
	// Hash commands
	//


	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hset(key, field, value));
				return null;
			}
			return client.hset(key, field, value).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hsetnx(key, field, value));
				return null;
			}
			return client.hsetnx(key, field, value).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean hDel(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hdel(key, new Object[] { field }));
				return null;
			}
			return client.hdel(key, new Object[] { field }).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hexists(key, field));
				return null;
			}
			return client.hexists(key, field).data() == 1;
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public byte[] hGet(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hget(key, field));
				return null;
			}
			return client.hget(key, field).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hgetall(key));
				return null;
			}
			return SrpUtils.toMap(client.hgetall(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hincrby(key, field, delta));
				return null;
			}
			return client.hincrby(key, field, delta).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hkeys(key));
				return null;
			}
			return SrpUtils.toSet(client.hkeys(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hlen(key));
				return null;
			}
			return client.hlen(key).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hmget(key, (Object[]) fields));
				return null;
			}
			return SrpUtils.toBytesList(client.hmget(key, (Object[]) fields).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hmset(key, SrpUtils.convert(tuple)));
				return;
			}
			client.hmset(key, SrpUtils.convert(tuple));
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public List<byte[]> hVals(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline(pipeline.hvals(key));
				return null;
			}
			return SrpUtils.toBytesList(client.hvals(key).data());
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	//
	// Pub/Sub functionality
	//

	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline(pipeline.publish(channel, message));
				return null;
			}
			return client.publish(channel, message).data();
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public Subscription getSubscription() {
		return subscription;
	}


	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}


	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		checkSubscription();

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new SrpSubscription(listener, client);
			subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}


	public void subscribe(MessageListener listener, byte[]... channels) {
		checkSubscription();

		try {
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new SrpSubscription(listener, client);
			subscription.subscribe(channels);

		} catch (Exception ex) {
			throw convertSrpAccessException(ex);
		}
	}

	private void checkSubscription() {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
	}

	// processing method that adds a listener to the future in order to track down the results and close the pipeline
	private void pipeline(ListenableFuture<? extends Reply> future) {
		callback.addCommand(future);
	}
}