package com.kafka.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Auther: Jethro
 * @Date: 2019/9/12 17:13
 * @Description:
 */
@Data
public class KafkaMessage {
    private Long id;    //id
    private String msg; //消息
    private Date sendTime;  //时间戳
}
