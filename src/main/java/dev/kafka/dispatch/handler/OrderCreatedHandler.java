package dev.kafka.dispatch.handler;

import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaListener(
            id =  "orderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(OrderCreated payload){
        log.info("received message: payload: "+ payload);
        try {
            dispatchService.process(payload);
        } catch(Exception e){
            log.error("Processing failure", e);
        }
    }
}
