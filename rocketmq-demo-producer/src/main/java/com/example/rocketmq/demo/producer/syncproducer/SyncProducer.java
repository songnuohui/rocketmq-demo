package com.example.rocketmq.demo.producer.syncproducer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * Producer端发送同步消息
 *
 * @author SongNuoHui
 * @date 2022/1/26 10:22
 */
public class SyncProducer {
  public static void main(String[] args)
      throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException,
          InterruptedException {
    // 实例化消息生产者Producer
    DefaultMQProducer producer = new DefaultMQProducer("sync_producer_unique_name");
    // 设置NameServer的地址
    producer.setNamesrvAddr("localhost:9876");
    // 启动Producer实例
    producer.start();
    for (int i = 0; i < 10; i++) {
      // 创建消息，并指定Topic，Tag和消息体
      Message msg =
          new Message(
              "TopicTestSync" /* Topic */,
              "TagA" /* Tag */,
              ("Hello group RocketMQ 5 " + i)
                  .getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);
      // 发送消息到一个Broker
      SendResult sendResult = producer.send(msg);
      // 通过sendResult返回消息是否成功送达
      System.out.printf("%s%n", sendResult);
    }
    // 如果不再发送消息，关闭Producer实例。
    producer.shutdown();
  }
}
