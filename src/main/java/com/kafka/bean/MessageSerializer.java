package com.kafka.bean;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Auther: Jethro
 * @Date: 2019/9/20 10:36
 * @Description:
 *  自定义序列化器：
 *  KafkaMessage -> byte[]
 */

public class MessageSerializer implements Serializer<KafkaMessage> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //不需要做任何配置
    }


    /**
     *
     * @param topic
     * @param kafkaMessage
     * @return
     *
     *  Customer对象被序列化成字节数组
     *  字节数组的构成：
     *  long 类型占8个字节
     *  所以
     *  第一部分：8字节整数代表id
     *  第二部分：4字节整数代表msg的长度  ,返回msg长度是为了根据长度获取msg内容
     *  第三部分：N个字节的数组代表msg的内容
     *  第四部分：8字节整数代表时间戳
     */
    @Override
    public byte[] serialize(String topic, KafkaMessage kafkaMessage) {
        try{
            byte[] serializedMsg;
            int msgSize;
            long time;
            if(kafkaMessage == null){
                return  null;
            }else {
                if(kafkaMessage.getMsg() != null){
                    serializedMsg = kafkaMessage.getMsg().getBytes("utf-8");
                    msgSize = serializedMsg.length;
                }else {
                    serializedMsg = new byte[0];
                    msgSize = 0;
                }

                if(kafkaMessage.getSendTime() != null){
                    time = kafkaMessage.getSendTime().getTime();
                }else {
                    time = -1;
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + msgSize + 8);
            buffer.putLong(kafkaMessage.getId());
            buffer.putInt(msgSize);
            buffer.put(serializedMsg);
            buffer.putLong(time);

            return buffer.array();
        }catch (Exception e){
            throw new SerializationException("Error when serializing KafkaMessage to byte[] " + e);
        }
    }

    @Override
    public void close() {
        //不需要关闭任何东西
    }
}
