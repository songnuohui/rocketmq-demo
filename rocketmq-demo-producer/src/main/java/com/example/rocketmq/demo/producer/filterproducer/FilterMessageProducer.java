package com.example.rocketmq.demo.producer.filterproducer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author SongNuoHui
 * @date 2022/2/11 15:30
 */
public class FilterMessageProducer {
  public static void main(String[] args)
      throws MQClientException, MQBrokerException, RemotingException, InterruptedException,
          UnsupportedEncodingException {
    //
    DefaultMQProducer producer = new DefaultMQProducer("filter_producer_unique_name");
    producer.setNamesrvAddr("127.0.0.1:9876");
    producer.start();
    for (int i = 0; i < 50; i++) {
      //
      Message msg =
          new Message(
              "TopicTestFilter",
              "TagA",
              ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
      // 发送消息时，你能通过putUserProperty来设置消息的属性
      msg.putUserProperty("a", String.valueOf(i));
      SendResult sendResult = producer.send(msg);
    }

    producer.shutdown();
  }
}
