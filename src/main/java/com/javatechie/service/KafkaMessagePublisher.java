package com.javatechie.service;

import com.javatechie.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;

@Service

public class KafkaMessagePublisher {

    private static final Logger log= LoggerFactory.getLogger(KafkaMessagePublisher.class);

    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String message){
        /*CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-demo2", message);
        future.whenComplete((result,ex)->{
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });*/

        ListenableFuture<SendResult<String,Object>> listenableFuture = template.send("test-topic1",message);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(message, ex);
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                handleSuccess(message);
            }
        });

    }

    public void sendEventsToTopic(Customer customer) {
        ListenableFuture<SendResult<String,Object>> listenableFuture = template.send("test-topic1",customer);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure1(customer, ex);
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                handleSuccess1(customer);
            }
        });
    }


    public void handleFailure1(Customer value,Throwable ex ){
        try {
            throw new KafkaException("Encountered an error while produceing student message "+ value,ex);

        } catch (Throwable e) {
            log.error("Error in handleFailure for message {}: {}",value,e.getMessage() );
        }
    }

    public void handleSuccess1(Customer  value) {
        log.info("Student request  Sent successfully for the value is {}",value);
    }



    public void handleFailure(String value,Throwable ex ){
        try {
            throw new KafkaException("Encountered an error while produceing student message "+ value,ex);

        } catch (Throwable e) {
            log.error("Error in handleFailure for message {}: {}",value,e.getMessage() );
        }
    }

    public void handleSuccess (String  value) {
        log.info("Student request  Sent successfully for the value is {}",value);
    }
}
