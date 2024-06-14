package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.concurrent.CompletableFuture;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DispatchServiceTest {


    private DispatchService service;
    private KafkaTemplate kafkaProducerMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        service = new DispatchService(kafkaProducerMock);
    }

    @Test
    void process_success() throws Exception{
        when(kafkaProducerMock.send(anyString(), any(DispatchPrepared.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducerMock.send(anyString(), ArgumentMatchers.any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));

        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        service.process(testEvent);

        verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), ArgumentMatchers.any(DispatchPrepared.class));
        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), ArgumentMatchers.any(OrderDispatched.class));
    }

    @Test
    public void testProcess_DispatchTrackingProducerThrowsException() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducerMock).send(eq("dispatch.tracking"), any(DispatchPrepared.class));

        Exception exception = assertThrows(RuntimeException.class, () -> service.process(testEvent));

        verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), any(DispatchPrepared.class));
        verifyNoMoreInteractions(kafkaProducerMock);
        assertThat(exception.getMessage(), equalTo("dispatch tracking producer failure"));
    }

    @Test
    public void testProcess_OrderDispatchedProducerThrowsException() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        when(kafkaProducerMock.send(anyString(), any(DispatchPrepared.class))).thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaProducerMock).send(eq("order.dispatched"), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> service.process(testEvent));

        verify(kafkaProducerMock, times(1)).send(eq("dispatch.tracking"), any(DispatchPrepared.class));
        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), any(OrderDispatched.class));
        assertThat(exception.getMessage(), equalTo("order dispatched producer failure"));
    }
}