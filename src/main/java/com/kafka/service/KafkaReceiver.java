package com.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.kafka.bean.IConfig.BOOTSTRAP_SERVERS;
import static com.kafka.bean.IConfig.TOPIC_NAME;

/**
 * @Auther: Jethro
 * @Date: 2019/9/12 17:19
 * @Description:
 */

@Slf4j
public class KafkaReceiver {
    private String name;
    private Consumer<String, String> consumer;
    private Properties getConsumerProperties(){
        Properties props = new Properties();
        //broker地址：建立与集群的连接，通过一个broker会自动发现集群中其他的broker,不用把集群中所有的broker地址列全，一般配置2-3个即可
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        //consumer_group id
        props.put("group.id", "gp-kafka-demo");

        //自动提交offset
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");

        //手动提交offset
        props.put("enable.auto.commit", "false");

        //consumer每次发起fetch请求时,从服务器读取的数据量
        //该属性指定
        props.put("max.partition.fetch.bytes", "1000");

        //一次最多poll多少个record
        props.put("max.poll.records", "5");

        //跟producer端一致(bytes->object)
        props.put("key.deserializer", StringDeserializer.class.getCanonicalName());

        //跟producer端一致
        props.put("value.deserializer", StringDeserializer.class.getCanonicalName());

        return props;
    }

    public KafkaReceiver(String name,boolean isGroupMode){
        this.name = name;
        Properties properties = getConsumerProperties();
        consumer = new KafkaConsumer<String, String>(properties);

        if (isGroupMode) {
            //群组模式：新增/移除消费者会触发再均衡
            consumer.subscribe(Arrays.asList(TOPIC_NAME));
        } else {
            //独立消费者模式：只消费指定的分区，新增/移除消费者不会触发再均衡
            List<TopicPartition> partitions = new ArrayList<>();
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(TOPIC_NAME);
            for (PartitionInfo p : partitionInfoList) {
                partitions.add(new TopicPartition(p.topic(), p.partition()));
            }
            consumer.assign(partitions);
        }
    }

    public void receive(){
        //kafka的消费者是一个无限循环的长期轮询，如果不持续轮询，则该消费者会被认为已死亡，则它负责的分区会被移交至其他消费者
        while (true) {
            try {
                //第四步：消费消息, 100是timeout时间，以毫秒为单位
                ConsumerRecords<String, String> records = consumer.poll(100);

                log.info("消费者：{} 获取数据量：" ,this.name, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    log.info("消费者：{} 开始消费数据:offset = {}, key = {}, value = {}", this.name, record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
                consumer.close();
            }
        }
    }

    public void close(){
        consumer.close();
    }


}
