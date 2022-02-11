package com.example.rocketmq.demo.consumer.filterconsumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 只有使用push模式的消费者才能用使用SQL92标准的sql语句; 用MessageSelector.bySql来使用sql筛选消息
 *
 * @author SongNuoHui
 * @date 2022/2/11 15:37
 */
public class FilterMessageConsumer {
  public static void main(String[] args) throws MQClientException {
    //
    DefaultMQPushConsumer consumer =
        new DefaultMQPushConsumer("filter_message_consumer_unique_name");

    consumer.setNamesrvAddr("127.0.0.1:9876");
    // 只有订阅的消息有这个属性a, a >=0 and a <= 3
    consumer.subscribe("TopicTestFilter", MessageSelector.bySql("a between 0 and 10"));

    consumer.registerMessageListener(
        new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(
              List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
            for (MessageExt msg : msgs) {
              //
              byte[] body = msg.getBody();
              String s = new String(body);
              System.out.println("消息体内容为：" + s);
            }

            System.out.printf(
                "%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
            // 标记该消息已经被成功消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
        });
    // 启动消费者实例
    consumer.start();
    System.out.printf("Consumer Started.%n");
  }
}
