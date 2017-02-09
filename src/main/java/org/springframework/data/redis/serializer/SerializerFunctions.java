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
package org.springframework.data.redis.serializer;

import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * Implementations of {@link SerializerFunction} and {@link DeserializerFunction} that provide various useful
 * serialization operations, such as reuse of {@link RedisSerializer}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class SerializerFunctions {

	public static <T> SerializerFunction<T> fromSerializer(RedisSerializer<T> serializer) {

		return object -> {

			if (serializer == null) {

				if (object instanceof byte[]) {
					return Mono.just(ByteBuffer.wrap((byte[]) object));
				}

				if (object instanceof ByteBuffer) {
					return Mono.just((ByteBuffer) object);
				}
			}

			return Mono.defer(() -> Mono.just(serializer.serialize((T) object)).map(ByteBuffer::wrap));
		};
	}

	public static <T> SerializerFunction<T> defaultSerializer() {

		return object -> {

			if (object instanceof byte[]) {
				return Mono.just(ByteBuffer.wrap((byte[]) object));
			}

			return Mono.just((ByteBuffer) object);
		};
	}

	public static <T> DeserializerFunction<T> defaultDeserializer() {
		return (data) -> Mono.just((T) data);
	}

	@SuppressWarnings("unchecked")
	public static <T> DeserializerFunction<T> fromDeserializer(RedisSerializer<T> serializer) {

		return byteBuffer -> {

			if (serializer == null) {
				return Mono.just((T) byteBuffer);
			}

			return Mono.defer(() -> {

				byte[] bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);

				return Mono.just(serializer.deserialize(bytes));
			});
		};
	}

}
