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

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveHashOperations}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultReactiveHashOperationsIntegrationTests<K, HK, HV> {

	private final ReactiveRedisTemplate<K, ?> redisTemplate;
	private final ReactiveHashOperations<K, HK, HV> hashOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;

	@Parameters(name = "{4}")
	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setPort(SettingsUtils.getPort());
		lettuceConnectionFactory.setHostName(SettingsUtils.getHost());
		lettuceConnectionFactory.afterPropertiesSet();

		ReactiveRedisTemplate<String, String> stringTemplate = new ReactiveRedisTemplate<>();
		stringTemplate.setConnectionFactory(lettuceConnectionFactory);
		stringTemplate.setEnableDefaultSerializer(true);
		stringTemplate.setDefaultSerializer(new StringRedisSerializer());
		stringTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<byte[], byte[]> rawTemplate = new ReactiveRedisTemplate<>();
		rawTemplate.setConnectionFactory(lettuceConnectionFactory);
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory, stringFactory, "String" },
				{ rawTemplate, rawFactory, rawFactory, rawFactory, "raw" } });
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	public DefaultReactiveHashOperationsIntegrationTests(ReactiveRedisTemplate<K, ?> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory,
			String testName) {

		this.redisTemplate = redisTemplate;
		this.hashOperations = redisTemplate.opsForHash();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Before
	public void before() {

		RedisConnection connection = redisTemplate.getConnectionFactory().getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-602
	public void shouldPutField() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldDeleteField() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);
		StepVerifier.create(hashOperations.delete(key, hashkey1, hashkey2)).expectNext(2L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldPutFieldIfAbsent() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		StepVerifier.create(hashOperations.putIfAbsent(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();
		StepVerifier.create(hashOperations.putIfAbsent(key, hashkey, hashvalue2)).expectNext(false).expectComplete()
				.verify();
	}

	@Test // DATAREDIS-602
	public void shouldGetField() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();

		StepVerifier.create(hashOperations.get(key, hashkey)).expectNextCount(1).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReturnHasKey() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();

		StepVerifier.create(hashOperations.hasKey(key, hashkey)).expectNext(true).expectComplete().verify();
		StepVerifier.create(hashOperations.hasKey(key, hashKeyFactory.instance())).expectNext(false).expectComplete()
				.verify();
	}

	@Test // DATAREDIS-602
	public void shouldPutAll() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.hasKey(key, hashkey1)).expectNext(true).expectComplete().verify();
		StepVerifier.create(hashOperations.hasKey(key, hashkey2)).expectNext(true).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldMultiGet() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.multiGet(key, Arrays.asList(hashkey1, hashkey2))).consumeNextWith(actual -> {
			assertThat(actual).hasSize(2).containsSequence(hashvalue1, hashvalue2);
		}).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReturnEntries() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.entries(key)).consumeNextWith(actual -> {
			assertThat(actual).hasSize(2).containsEntry(hashkey1, hashvalue1).containsEntry(hashkey2, hashvalue2);
		}).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldIncrementValueLong() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();
		StepVerifier.create(hashOperations.increment(key, hashkey, 1L)).expectNext(2L).expectComplete().verify();
		StepVerifier.create(hashOperations.get(key, hashkey)).expectNext((HV) "2").expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldIncrementValueDouble() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)).expectNext(true).expectComplete().verify();
		StepVerifier.create(hashOperations.increment(key, hashkey, 1.1d)).expectNext(2.1d).expectComplete().verify();
		StepVerifier.create(hashOperations.get(key, hashkey)).expectNext((HV) "2.1").expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReturnKeys() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.keys(key)).consumeNextWith(actual -> {
			assertThat(actual).hasSize(2).contains(hashkey1, hashkey2);
		}).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReturnValues() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.values(key)).consumeNextWith(actual -> {
			assertThat(actual).hasSize(2).contains(hashvalue1, hashvalue2);
		}).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReturnSize() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.size(key)).expectNext(2L).expectComplete().verify();
	}

	private void putAll(K key, HK hashkey1, HV hashvalue1, HK hashkey2, HV hashvalue2) {

		Map<HK, HV> map = new HashMap<>();
		map.put(hashkey1, hashvalue1);
		map.put(hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.putAll(key, map)).expectNext(true).expectComplete().verify();
	}
}
