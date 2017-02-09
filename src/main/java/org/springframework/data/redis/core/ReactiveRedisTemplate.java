/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.DeserializerFunction;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializerFunction;
import org.springframework.data.redis.serializer.SerializerFunctions;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Central abstraction for reactive Redis data access.
 * <p/>
 * Performs automatic serialization/deserialization between the given objects and the underlying binary data in the
 * Redis store. By default, it uses Java serialization for its objects (through {@link JdkSerializationRedisSerializer}
 * ).
 * <p/>
 * Once configured, this class is thread-safe.
 * <p/>
 * Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given
 * Objects to and from binary data.
 *
 * @author Mark Paluch
 * @since 2.0
 * @param <K> the Redis key type against which the template works (usually a String)
 * @param <V> the Redis value type against which the template works
 */
public class ReactiveRedisTemplate<K, V> extends RedisAccessor
		implements BeanClassLoaderAware, ReactiveOperationsSupport<K, V>, ReactiveRedisOperations<K, V> {

	private boolean exposeConnection = true;
	private boolean initialized = false;
	private boolean enableDefaultSerializer = true;
	private RedisSerializer<?> defaultSerializer;
	private ClassLoader classLoader;

	private RedisSerializer<K> keySerializer = null;
	private RedisSerializer<V> valueSerializer = null;
	private RedisSerializer<?> hashKeySerializer = null;
	private RedisSerializer<?> hashValueSerializer = null;
	private RedisSerializer<String> stringSerializer = new StringRedisSerializer();

	private SerializerFunction<K> toKey = SerializerFunctions.defaultSerializer();
	private DeserializerFunction<K> fromKey = SerializerFunctions.defaultDeserializer();
	private SerializerFunction<V> toValue = SerializerFunctions.defaultSerializer();
	private DeserializerFunction<V> fromValue = SerializerFunctions.defaultDeserializer();

	// cache singleton objects (where possible)
	private ReactiveValueOperations<K, V> valueOps;

	/**
	 * Construct a new {@link ReactiveRedisTemplate} instance.
	 */
	public ReactiveRedisTemplate() {}

	/**
	 * @param enableDefaultSerializer Whether or not the default serializer should be used. If not, any serializers not
	 *          explicilty set will remain null and values will not be serialized or deserialized.
	 */
	public void setEnableDefaultSerializer(boolean enableDefaultSerializer) {
		this.enableDefaultSerializer = enableDefaultSerializer;
	}

	/**
	 * Returns the default serializer used by this template.
	 *
	 * @return template default serializer
	 */
	public RedisSerializer<?> getDefaultSerializer() {
		return defaultSerializer;
	}

	/**
	 * Sets the default serializer to use for this template. All serializers (expect the
	 * {@link #setStringSerializer(RedisSerializer)}) are initialized to this value unless explicitly set. Defaults to
	 * {@link JdkSerializationRedisSerializer}.
	 *
	 * @param serializer default serializer to use
	 */
	public void setDefaultSerializer(RedisSerializer<?> serializer) {
		this.defaultSerializer = serializer;
	}

	/**
	 * Sets the key serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 *
	 * @param serializer the key serializer to be used by this template.
	 */
	public void setKeySerializer(RedisSerializer<?> serializer) {

		this.keySerializer = (RedisSerializer) serializer;
		this.toKey = SerializerFunctions.fromSerializer(this.keySerializer);
		this.fromKey = SerializerFunctions.fromDeserializer(this.keySerializer);
	}

	/**
	 * Returns the key serializer used by this template.
	 *
	 * @return the key serializer used by this template.
	 */
	public RedisSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Sets the value serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 *
	 * @param serializer the value serializer to be used by this template.
	 */
	public void setValueSerializer(RedisSerializer<?> serializer) {

		this.valueSerializer = (RedisSerializer) serializer;
		this.toValue = SerializerFunctions.fromSerializer(this.valueSerializer);
		this.fromValue = SerializerFunctions.fromDeserializer(this.valueSerializer);
	}

	/**
	 * Returns the value serializer used by this template.
	 *
	 * @return the value serializer used by this template.
	 */
	public RedisSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Returns the hashKeySerializer.
	 *
	 * @return Returns the hashKeySerializer
	 */
	public RedisSerializer<?> getHashKeySerializer() {
		return hashKeySerializer;
	}

	/**
	 * Sets the hash key (or field) serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 *
	 * @param hashKeySerializer The hashKeySerializer to set.
	 */
	public void setHashKeySerializer(RedisSerializer<?> hashKeySerializer) {
		this.hashKeySerializer = hashKeySerializer;
	}

	/**
	 * Returns the hashValueSerializer.
	 *
	 * @return Returns the hashValueSerializer
	 */
	public RedisSerializer<?> getHashValueSerializer() {
		return hashValueSerializer;
	}

	/**
	 * Sets the hash value serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 *
	 * @param hashValueSerializer The hashValueSerializer to set.
	 */
	public void setHashValueSerializer(RedisSerializer<?> hashValueSerializer) {
		this.hashValueSerializer = hashValueSerializer;
	}

	/**
	 * Returns the stringSerializer.
	 *
	 * @return Returns the stringSerializer
	 */
	public RedisSerializer<String> getStringSerializer() {
		return stringSerializer;
	}

	/**
	 * Sets the string value serializer to be used by this template (when the arguments or return types are always
	 * strings). Defaults to {@link StringRedisSerializer}.
	 *
	 * @see ValueOperations#get(Object, long, long)
	 * @param stringSerializer The stringValueSerializer to set.
	 */
	public void setStringSerializer(RedisSerializer<String> stringSerializer) {
		this.stringSerializer = stringSerializer;
	}

	/**
	 * Set the {@link ClassLoader} to be used for the default {@link JdkSerializationRedisSerializer} in case no other
	 * {@link RedisSerializer} is explicitly set as the default one.
	 *
	 * @param classLoader can be {@literal null}.
	 * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public void afterPropertiesSet() {

		super.afterPropertiesSet();

		boolean defaultUsed = false;

		if (defaultSerializer == null) {

			defaultSerializer = new JdkSerializationRedisSerializer(
					classLoader != null ? classLoader : this.getClass().getClassLoader());
		}

		if (enableDefaultSerializer) {

			if (keySerializer == null) {
				keySerializer = (RedisSerializer) defaultSerializer;
				toKey = SerializerFunctions.fromSerializer(keySerializer);
				fromKey = SerializerFunctions.fromDeserializer(keySerializer);
				defaultUsed = true;
			}
			if (valueSerializer == null) {
				valueSerializer = (RedisSerializer) defaultSerializer;
				toValue = SerializerFunctions.fromSerializer(valueSerializer);
				fromValue = SerializerFunctions.fromDeserializer(valueSerializer);
				defaultUsed = true;
			}
			if (hashKeySerializer == null) {
				hashKeySerializer = defaultSerializer;
				defaultUsed = true;
			}
			if (hashValueSerializer == null) {
				hashValueSerializer = defaultSerializer;
				defaultUsed = true;
			}
		}

		if (enableDefaultSerializer && defaultUsed) {
			Assert.notNull(defaultSerializer, "default serializer null and not all serializers initialized");
		}

		initialized = true;
	}

	@Override
	public ReactiveValueOperations<K, V> opsForValue() {

		if (valueOps == null) {
			valueOps = new DefaultReactiveValueOperations<>(this);
		}

		return valueOps;
	}

	// -------------------------------------------------------------------------
	// Execution methods
	// -------------------------------------------------------------------------

	public <T> Flux<T> execute(ReactiveRedisCallback<T> action) {
		return execute(action, exposeConnection);
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection can
	 * be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 *
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @return object returned by the action
	 */
	public <T> Flux<T> execute(ReactiveRedisCallback<T> action, boolean exposeConnection) {

		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		ReactiveRedisConnection conn = factory.getReactiveConnection();

		try {

			ReactiveRedisConnection connToUse = preProcessConnection(conn, false);

			ReactiveRedisConnection connToExpose = (exposeConnection ? connToUse : createRedisConnectionProxy(connToUse));
			Publisher<T> result = action.doInRedis(connToExpose);

			return Flux.from(postProcessResult(result, connToUse, false));
		} finally {
			conn.close();
		}
	}

	/**
	 * Create a reusable Flux for a {@link ReactiveRedisCallback}. Callback is executed within a connection context. The
	 * connection is released outside the callback.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Flux} wrapping the {@link ReactiveRedisCallback}.
	 */
	public <T> Flux<T> createFlux(ReactiveRedisCallback<T> callback) {

		Assert.notNull(callback, "ReactiveRedisCallback must not be null!");

		return Flux.defer(() -> doInConnection(callback, exposeConnection));
	}

	/**
	 * Create a reusable Mono for a {@link ReactiveRedisCallback}. Callback is executed within a connection context. The
	 * connection is released outside the callback.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Mono} wrapping the {@link ReactiveRedisCallback}.
	 */
	public <T> Mono<T> createMono(final ReactiveRedisCallback<T> callback) {

		Assert.notNull(callback, "ReactiveRedisCallback must not be null!");

		return Mono.defer(() -> Mono.from(doInConnection(callback, exposeConnection)));
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection can
	 * be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 *
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @return object returned by the action
	 */
	private <T> Publisher<T> doInConnection(ReactiveRedisCallback<T> action, boolean exposeConnection) {

		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		ReactiveRedisConnection conn = factory.getReactiveConnection();

		ReactiveRedisConnection connToUse = preProcessConnection(conn, false);

		ReactiveRedisConnection connToExpose = (exposeConnection ? connToUse : createRedisConnectionProxy(connToUse));
		Publisher<T> result = action.doInRedis(connToExpose);

		return Flux.from(postProcessResult(result, connToUse, false)).doAfterTerminate(conn::close);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with Redis keys
	// -------------------------------------------------------------------------

	public Mono<Boolean> hasKey(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> key(Argument.just(key)).flatMap(rawKey -> connection.keyCommands().exists(rawKey)));
	}

	public Mono<DataType> type(K key) {

		Assert.notNull(key, "Key must not be null!");

		return createMono(connection -> key(Argument.just(key)).flatMap(rawKey -> connection.keyCommands().type(rawKey)));

	}

	public Flux<K> keys(K pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return createFlux(connection -> key(Argument.just(pattern)) //
				.flatMap(rawKey -> connection.keyCommands().keys(rawKey)) //
				.flatMap(Flux::fromIterable) //
				.flatMap(byteBuffer -> fromKey.deserialize(byteBuffer)));
	}

	public Mono<K> randomKey() {
		return createMono(connection -> connection.keyCommands().randomKey())
				.flatMap(byteBuffer -> fromKey.deserialize(byteBuffer)).next();
	}

	public Mono<Boolean> rename(K oldKey, K newKey) {

		Assert.notNull(oldKey, "Old key must not be null!");
		Assert.notNull(newKey, "New Key must not be null!");

		return createMono(connection -> Mono.when(key(Argument.just(oldKey)), key(Argument.just(newKey)))
				.flatMap(t -> connection.keyCommands().rename(t.getT1(), t.getT2())));
	}

	public Mono<Boolean> renameIfAbsent(K oldKey, K newKey) {

		Assert.notNull(oldKey, "Old key must not be null!");
		Assert.notNull(newKey, "New Key must not be null!");

		return createMono(connection -> Mono.when(key(Argument.just(oldKey)), key(Argument.just(newKey)))
				.flatMap(t -> connection.keyCommands().renameNX(t.getT1(), t.getT2())));
	}

	public Mono<Long> delete(K... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		if (keys.length == 1) {
			return createMono(connection -> key(Argument.just(keys[0])) //
					.flatMap(rawKey -> connection.keyCommands().del(rawKey)));
		}

		Mono<List<ByteBuffer>> listOfKeys = key(Arguments.fromArray(keys)).collectList();
		return createMono(connection -> listOfKeys.flatMap(rawKeys -> connection.keyCommands().mDel(rawKeys)));
	}

	public Mono<Long> delete(Publisher<K> keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return createMono(connection -> connection.keyCommands() //
				.del(key(Arguments.from(keys)).map(KeyCommand::new)) //
				.map(CommandResponse::getOutput));
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Processes the connection (before any settings are executed on it). Default implementation returns the connection as
	 * is.
	 *
	 * @param connection must not be {@literal null}.
	 * @param existingConnection
	 */
	protected ReactiveRedisConnection preProcessConnection(ReactiveRedisConnection connection,
			boolean existingConnection) {
		return connection;
	}

	/**
	 * Processes the result before returning the {@link Publisher}. Default implementation returns the result as is.
	 * 
	 * @param result must not be {@literal null}.
	 * @param connection must not be {@literal null}.
	 * @param existingConnection
	 * @return
	 */
	protected <T> Publisher<T> postProcessResult(Publisher<T> result, ReactiveRedisConnection connection,
			boolean existingConnection) {
		return result;
	}

	protected ReactiveRedisConnection createRedisConnectionProxy(ReactiveRedisConnection reactiveRedisConnection) {

		Class<?>[] ifcs = ClassUtils.getAllInterfacesForClass(reactiveRedisConnection.getClass(),
				getClass().getClassLoader());
		return (ReactiveRedisConnection) Proxy.newProxyInstance(reactiveRedisConnection.getClass().getClassLoader(), ifcs,
				new CloseSuppressingInvocationHandler(reactiveRedisConnection));
	}

	@Override
	public SerializerFunction<K> toKey() {
		return toKey;
	}

	@Override
	public SerializerFunction<V> toValue() {
		return toValue;
	}
}
