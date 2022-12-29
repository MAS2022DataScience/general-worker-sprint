package com.mas2022datascience.generalworkersprint.processor;

import static java.time.Instant.now;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

import com.google.common.primitives.Bytes;
import com.mas2022datascience.avro.v1.Frame;
import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.PlayerBall;
import com.mas2022datascience.avro.v1.Sprint;
import com.mas2022datascience.avro.v1.Ticker;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.general-player-ball.name}") private String topicIn;
  @Value(value = "${topic.general-events.name}") private String topicOut;

  @Value(value = "${sprint.parameter.session-length}") private long sessionLength;
  @Value(value = "${sprint.parameter.session-grace-time}") private long sessionGraceTime;
  @Value(value = "${sprint.parameter.velocity-threshold}") private long velocityThreshold;
  @Value(value = "${sprint.parameter.acceleration-threshold}") private long accelerationThreshold;
  @Value(value = "${sprint.parameter.min-sprint-length}") private long minSprintLength;

  @Bean
  public KStream<String, PlayerBall> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);

    final Serde<PlayerBall> playerBallSerde = new SpecificAvroSerde<>();
    playerBallSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<Ticker> tickerSerde = new SpecificAvroSerde<>();
    tickerSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<Sprint> sprintSerde = new SpecificAvroSerde<>();
    sprintSerde.configure(serdeConfig, false); // `false` for record values

    final StoreBuilder<KeyValueStore<String, PlayerBall>> sprintStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("SprintStore"),
            Serdes.String(), playerBallSerde);
    kStreamBuilder.addStateStore(sprintStore);

    SessionWindows sessionWindow = SessionWindows.with(
        Duration.ofMillis(sessionLength)).grace(Duration.ofMillis(sessionGraceTime));

    KStream<String, PlayerBall> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), playerBallSerde)
            .withTimestampExtractor(new PlayerBallEventTimestampExtractor()));

//    {
//      "ts": "2019-06-05T18:46:49.483Z", --> instant
//        "id": "250085369",
//        "x": 1605,
//        "y": -517,
//        "z": 0,
//        "velocity": 1.8027756377319946,
//        "distance": 0.07211102550927978
//    }

//    SELECT objectId,
//    max(id) AS OBJECT,
//    round(min(velocity),4) AS Vmin,
//    round(max(velocity),4) AS Vmax,
//    round(min(accelleration),4) AS Amin,
//    round(max(accelleration),4) AS Amax,
//    TIMESTAMPTOSTRING(WINDOWSTART,'yyyy-MM-dd HH:mm:ss.SSS', 'UTC') AS SESSION_START_TS,
//    TIMESTAMPTOSTRING(WINDOWEND,'yyyy-MM-dd HH:mm:ss.SSS', 'UTC')   AS SESSION_END_TS,
//    COUNT(*)                                                    AS TICK_COUNT,
//    WINDOWEND - WINDOWSTART                                     AS SESSION_LENGTH_MS
//    FROM sPlayerBallV01
//    WINDOW SESSION (1000 MILLISECONDS)
//    WHERE id = '1905363' and velocity > 2 and accelleration > 0
//    GROUP BY objectId
//    EMIT FINAL
//    LIMIT 40;

    KGroupedStream<String, PlayerBall> grouped = stream
        .filter((k, v) -> {
          if (v.getVelocity() == null || v.getAccelleration() == null) return false;
          return v.getVelocity() > velocityThreshold && v.getAccelleration() > accelerationThreshold;
        })
        .groupByKey(Grouped.with(Serdes.String(), playerBallSerde));


//    timestamp_ms ts;
//    string player_id;
//    double v_min;
//    double v_max;
//    double a_min;
//    double a_max;
//    string  session_start_ts;
//    string  session_end_ts;
//    int tick_count;
//    int session_length_ms;

    Aggregator<String, Long, Sprint> aggregator = (key, value, aggValue) -> {
      aggValue.setVMin(Math.min(aggValue.getVMin(), value));
      aggValue.setVMax(Math.max(aggValue.getVMax(), value));
      aggValue.setAMin(Math.min(aggValue.getAMin(), value));
      aggValue.setAMax(Math.max(aggValue.getAMax(), value));
      return aggValue;
    };

    Merger<String, Sprint> merger = (key, value1, value2) -> {
      return Sprint.newBuilder()
          .setVMax(Math.max(value1.getVMax(), value2.getVMax()))
          .setVMin(Math.min(value1.getVMin(), value2.getVMin()))
          .setAMax(Math.max(value1.getAMax(), value2.getAMax()))
          .setAMin(Math.min(value1.getAMin(), value2.getAMin()))
          .build();
    };

    KTable<Windowed<String>, Sprint> sumOfValues = grouped
        .windowedBy(sessionWindow)
        .aggregate(
            // initializer
            () -> Sprint.newBuilder()
                .setTs(Instant.now())
                .setPlayerId("")
                .setVMin(Long.MAX_VALUE)
                .setVMax(Long.MIN_VALUE)
                .setAMin(Long.MAX_VALUE)
                .setAMax(Long.MIN_VALUE)
                .setSessionStartTs("")
                .setSessionEndTs("")
                .setTickCount(0)
                .setSessionLengthMs(0)
                .build(),
            // aggregator
            aggregator,
            // session merger
            merger,
            // serializer
            Materialized.<String, Sprint, SessionStore<Bytes, byte[]>>as("sprint-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(sprintSerde)
        )
        .suppress(untilWindowCloses(unbounded())); // suppress until window closes
        //.filter((k, v) -> (k.window().end() - k.window().start() > minSprintLength));
//
//    kTable.toStream()
//        .foreach((k,v) -> {
//      System.out.println("Key: " + k.key() + " Window: " + Instant.ofEpochMilli(k.window().start()).atOffset(ZoneOffset.UTC) + " - " + Instant.ofEpochMilli(k.window().end()).atOffset(ZoneOffset.UTC) + " Count: " + v + " Length: " + (k.window().end()-k.window().start()));
//    });

    // publish result
//    sprintStream.to(topicOut);

    return stream;

  }
}


