package com.kafka.bean;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

/**
 * @Auther: Jethro
 * @Date: 2019/9/20 11:00
 * @Description:
 *  自定义反序列化器
 *
 *  byte[] -> KafkaMessage
 */
public class MessageDeserializer implements Deserializer<KafkaMessage> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //不需要做任何配置
    }

    @Override
    public KafkaMessage deserialize(String s, byte[] data) {
        long id;
        long time;
        String msg;
        int msgSize;
        try{
            if(data == null){
                return  null;
            }
            if (data.length < 8) {
                throw new SerializationException("Size of data recedived is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);

            //按照顺序取出相应数据

            //id
            id = buffer.getLong();
            //msg长度
            msgSize = buffer.getInt();
            //msg内容
            byte[] msgBytes = new byte[msgSize];
            buffer.get(msgBytes);
            msg = new String(msgBytes,"utf-8");
            //发送时间
            time = buffer.getLong();

            KafkaMessage kafkaMessage = new KafkaMessage();

            kafkaMessage.setId(id);;
            kafkaMessage.setSendTime(timeToDate(time));
            kafkaMessage.setMsg(msg);
            return kafkaMessage;

        }catch (Exception e){
            throw new SerializationException("Error when deserializing byte[] to KafkaMessage " + e);
        }
    }

    private Date timeToDate(long time){
        if(time == -1){
            return  null;
        }else {
            return new Date(time);
        }
    }

    @Override
    public void close() {
        //不需要关闭任何东西
    }
}
