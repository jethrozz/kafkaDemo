package com.kafka.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.bean.IConfig;
import com.kafka.bean.KafkaMessage;
import com.kafka.bean.MessageSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * @Auther: Jethro
 * @Date: 2019/9/12 17:14
 * @Description:
 */

@Slf4j
public class KafkaSender implements Callback {
    private  KafkaProducer<String, KafkaMessage> producer;
    private Properties getProducerProperties(){
        Properties props = new Properties();
        //broker地址：建立与集群的连接，通过一个broker会自动发现集群中其他的broker,不用把集群中所有的broker地址列全，一般配置2-3个即可
        props.put("bootstrap.servers", IConfig.BOOTSTRAP_SERVERS);

        //是否确认broker完全接收消息：[0, 1, all]
        /**
         * 0 :生产者成功写入消息之前不会等待任何来自服务器的响应
         * 1 :只要集群的首领节点收到消息，生产者就会收到一个来自服务器的成功响应
         * all :只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应
         */
        props.put("acks", "all");

        //失败后消息重发的次数：可能导致消息的次序发生变化
        props.put("retries", 2);

        //单批发送的数据条数
        //有多个消息需要发送到同一个分区时，生产者会把他们放在同一个批次，该参数表示这个批次里可以发送的数据的大小
        props.put("batch.size", 16384);

        //数据在producer端停留的时间：设置为0，则立即发送
        props.put("linger.ms", 1);

        //数据缓存大小
        //该参数设置生产者内存缓冲区的大小
        props.put("buffer.memory", 33554432);

        //key序列化方式
        props.put("key.serializer", StringSerializer.class.getCanonicalName());

        //value序列化方式
        props.put("value.serializer", MessageSerializer.class.getCanonicalName());

        //重新设置分区器
        //props.put("partitioner.class", CustomerPartitioner.class.getCanonicalName());
        return props;
    }

    public KafkaSender(){
        //获取创建属性
        Properties props = getProducerProperties();
        //初始化KafkaProducer
        producer = new KafkaProducer(props);
    }

    //发送消息方法
    public void send() {
        KafkaMessage message = new KafkaMessage();
        message.setId(System.currentTimeMillis());
        message.setMsg("123456");
        message.setSendTime(new Date());
        ProducerRecord<String,KafkaMessage> record = new ProducerRecord<String,KafkaMessage>(IConfig.TOPIC_NAME,message);
        producer.send(record);
        //producer.close();
    }

    public void send(ProducerRecord<String,KafkaMessage> record) {
        producer.send(record);
        //producer.close();
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null){
            log.error(e.getMessage());
        }
    }

}
