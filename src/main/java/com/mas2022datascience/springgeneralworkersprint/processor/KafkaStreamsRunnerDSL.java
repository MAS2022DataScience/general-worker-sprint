package com.mas2022datascience.springgeneralworkersprint.processor;

import com.mas2022datascience.avro.v1.Frame;
import com.mas2022datascience.avro.v1.Object;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.tracab-01.name}") private String topicIn;

  @Value(value = "${topic.tracab-02.name}") private String topicOut;

  @Bean
  public KStream<String, Frame> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);
    final Serde<Frame> frameSerde = new SpecificAvroSerde<>();
    frameSerde.configure(serdeConfig, false); // `false` for record values

    KStream<String, Frame> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), frameSerde));

    final StoreBuilder<KeyValueStore<String, Frame>> myStateStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("MyStateStore"),
            Serdes.String(), frameSerde);
    kStreamBuilder.addStateStore(myStateStore);

    final MyStateHandler myStateHandler = new MyStateHandler(myStateStore.name());

    // invoke the transformer
    KStream<String, Frame> transformedStream = stream.transform(() -> myStateHandler, myStateStore.name());

    // peek into the stream and execute a println
    //transformedStream.peek((k,v) -> System.out.println("key: " + k + " - value:" + v));

    // publish result
    transformedStream.to(topicOut);

    return stream;

  }

  private static final class MyStateHandler implements
      Transformer<String, Frame, KeyValue<String, Frame>> {
    final private String storeName;
    private KeyValueStore<String, Frame> stateStore;

    public MyStateHandler(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      stateStore = processorContext.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, Frame> transform(String key, Frame value) {
      try {
        if (stateStore.get(key) == null) {
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        }
      } catch (org.apache.kafka.common.errors.SerializationException ex) {
        // the first time the state store is empty, so we get a serialization exception
        stateStore.put(key, value);
        return new KeyValue<>(key, stateStore.get(key));
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      Frame oldFrame = stateStore.get(key);
      HashMap<String, Object> oldObjectsMap = new HashMap<>();
      oldFrame.getObjects().forEach( object ->
          oldObjectsMap.put(object.getId(), object)
      );

      List<Object> actualObjects = value.getObjects();
      // update velocity and acceleration to actual object
      actualObjects.forEach( actualObject -> {
        if (oldObjectsMap.containsKey(actualObject.getId())) {
          actualObject.setVelocity(calcVelocity(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getId()),
              oldFrame.getUtc()).orElse(null));
          actualObject.setAccelleration(calcAcceleration(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getId()),
              oldFrame.getUtc()).orElse(null));
          actualObject.setDistance(getEuclidianDistance(actualObject,
              oldObjectsMap.get(actualObject.getId())).orElse(null));
        }
      });

      stateStore.put(key, value);
      return new KeyValue<>(key, stateStore.get(key));

    }

    @Override
    public void close() { }
  }

  /**
   * calculates the euclidian distance [m] between two points in a 3 dimensional space
   *
   * @param actualObject  of type Object
   *                      <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldObject     of type Object
   *                      <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @return distance     of type double in meters
   *                      between 2 points in a 3 dimensional space
   */
  private static Optional<Double> getEuclidianDistance(Object actualObject, Object oldObject) {
    // represents the divisor that is needed to get m. Ex. cm to m means 100 as 100cm is 1m
    int distanceUnitDivisor = 100;
    return Optional.of(
        Math.sqrt(
          Math.pow(oldObject.getX()-actualObject.getX(), 2) + Math.pow(oldObject.getY()-actualObject.getY(), 2)
            + Math.pow(oldObject.getZ()-actualObject.getZ(), 2)
        ) / distanceUnitDivisor
    );
  }

  /**
   * calculates the time difference in seconds
   * @param actualUtc UTC time as a String
   * @param oldUtc UTC time as a String
   * @return time difference in seconds
   */
  private static double getTimeDifference(String actualUtc, String oldUtc) {
    // represents the divisor that is needed to get s. Ex. ms to s means 1000 as 1000ms is 1s
    int timeUnitDivisor = 1000;
    return (utcString2epocMs(actualUtc) - utcString2epocMs(oldUtc))/timeUnitDivisor;
  }

  /**
   * calculates the velocity between two points
   * math: velocity = delta distance [m] / delta time [s] (linear velocity)
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return Optional velocity [m/s] or empty
   */
  private static Optional<Double> calcVelocity(Object actualObject, String actualUtc,
      Object oldObject, String oldUtc) {
    double timeDifference = getTimeDifference(actualUtc, oldUtc);
    Optional<Double> distanceDifference = getEuclidianDistance(actualObject, oldObject);

    if (distanceDifference.isPresent()) {
      if (timeDifference == 0) {
        return Optional.empty();
      } else {
        return Optional.of(distanceDifference.get() / timeDifference);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * calculates the acceleration between to points
   * math: acceleration [m/s^2] = delta velocity [m/s]/ delta time [s] (linear acceleration)
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return Optional acceleration [m/s^2] or empty
   */
  private static Optional<Double> calcAcceleration(Object actualObject, String actualUtc,
      Object oldObject, String oldUtc) {
    double timeDifference = getTimeDifference(actualUtc, oldUtc);

    if (oldObject.getVelocity() == null || actualObject.getVelocity() == null || timeDifference == 0 ) {
      return Optional.empty();
    } else {
      double velocityDifference = actualObject.getVelocity() - oldObject.getVelocity();
      return Optional.of(velocityDifference / timeDifference);
    }
  }

  /**
   * Converts the utc string of type "yyyy-MM-dd'T'HH:mm:ss.SSS" to epoc time in milliseconds.
   * @param utcString of type String of format 'yyyy-MM-dd'T'HH:mm:ss.SSS'
   * @return epoc time in milliseconds
   */
  private static double utcString2epocMs(String utcString) {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC);

    return Instant.from(fmt.parse(utcString)).toEpochMilli();
  }

}


