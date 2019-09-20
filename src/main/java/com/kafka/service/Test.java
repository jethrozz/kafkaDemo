package com.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * @Auther: Jethro
 * @Date: 2019/9/12 17:29
 * @Description:
 */
@Slf4j
public class Test {


    public static void main(String[] args) {
        log.info("begin");
        KafkaSender kafkaSender = new KafkaSender();

        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    kafkaSender.send();
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        KafkaReceiver kafkaConsumer1 = new KafkaReceiver("C1",true);
       // KafkaReceiver kafkaConsumer2 = new KafkaReceiver("C2",true);
        //KafkaReceiver kafkaConsumer3 = new KafkaReceiver("C3",true);

        //th.start();

        Thread th2 = new Thread(new Runnable() {
            @Override
            public void run() {
                kafkaConsumer1.receive();

            }
        });

       // th2.start();

        kafkaConsumer1.close();
    }
}
