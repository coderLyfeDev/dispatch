package dev.kafka.dispatch.handler;

import dev.kafka.dispatch.exception.NotRetryableException;
import dev.kafka.dispatch.exception.RetryableException;
import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaListener(
            id =  "orderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.cons",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload){
        log.info("received message: partition: "+ partition + " - key: "+key+" - payload: "+ payload);
        try {
            dispatchService.process(key, payload);
        } catch(RetryableException e){
            log.warn("Retryable exception: " + e.getMessage());
            throw e;
        } catch(Exception e){
            log.error("NotRetryable Exception" + e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
