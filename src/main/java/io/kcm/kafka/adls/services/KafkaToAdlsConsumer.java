package io.kcm.kafka.adls.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaToAdlsConsumer {

    private final AdlsWriterService adlsWriterService;
    private final String topic;

    public KafkaToAdlsConsumer(AdlsWriterService adlsWriterService,
                               @Value("${app.kafka.topic}") String topic) {
        this.adlsWriterService = adlsWriterService;
        this.topic = topic;
    }

    @KafkaListener(topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            batch = "true")
    public void consume(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) return;

        StringBuilder sb = new StringBuilder();
        Integer partition = records.get(0).partition();
        Long beginOffset = records.get(0).offset();
        String line;
        for (var record : records) {
            String key = record.key();
            String value = record.value();

            // Ici tu peux transformer le message, ajouter du JSON, etc.
            line = String.format(
                    "{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"key\":%s,\"value\":%s}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    key == null ? "null" : "\"" + key + "\"",
                    value == null ? "null" : "\"" + value.replace("\"", "\\\"") + "\""
            );
            sb.append(line).append("\n");
        }

        adlsWriterService.writeNewFile(topic, partition, beginOffset, sb.toString());
    }
}

