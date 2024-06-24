package dev.kafka.dispatch.service;

import dev.kafka.dispatch.client.StockServiceClient;
import dev.kafka.dispatch.message.DispatchCompleted;
import dev.kafka.dispatch.message.DispatchPrepared;
import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;

import static java.util.UUID.randomUUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class DispatchService {

    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String,Object> kafkaProducer;
    private static final UUID APPLICATION_ID = randomUUID();

    private final StockServiceClient stockServiceClient;
    public void process(String key, OrderCreated orderCreated) throws Exception{

        String available = stockServiceClient.checkAvailability(orderCreated.getItem());

        if(Boolean.valueOf(available)){
            DispatchPrepared dispatchPrepared = DispatchPrepared.builder()
                    .orderId(orderCreated.getOrderId())
                    .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchPrepared).get();

            OrderDispatched orderDispatched = OrderDispatched.builder()
                    .orderId(orderCreated.getOrderId())
                    .processedBy(APPLICATION_ID)
                    .notes("Dispatched: "+ orderCreated.getItem())
                    .build();
            kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();

            DispatchCompleted dispatchCompleted = DispatchCompleted.builder()
                    .orderId(orderCreated.getOrderId())
                    .date(LocalDate.now().toString())
                    .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchCompleted).get();

            log.info("Sent Message: key: "+ key +" - orderID: " + orderCreated.getOrderId() + " - processedByID: "+APPLICATION_ID);
        }else{
            log.info("ITem "+ orderCreated.getItem() + " is unavailable");
        }


    }
}
