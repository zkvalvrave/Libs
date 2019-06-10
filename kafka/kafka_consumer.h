//
// Created by root on 18-11-4.
//

#ifndef KAFKA_KAFKA_CONSUMER_H
#define KAFKA_KAFKA_CONSUMER_H

#include <string>
#include <rdkafka.h>

/*************************************************
Copyright:sensenets
Author: zengkun
Date:2018-12-27
Description: kafka 消费者, 从kafka 中消费数据
**************************************************/
enum Kafka_Offset {
	KAFKA_EARLIEST_OFFSET,
	KAFKA_LATEST_OFFSET
};

class Kafka_Consumer
{
public:
	Kafka_Consumer();
	~Kafka_Consumer();

	/*
	 *  @brief 指定地址和topic初始化链接kafka, 也可以基于sasl_plaintext 协议, 加入用户名和密码链接; 如果用户名, 密码为空, 表示不用验证登录
	 *  @author zengkun
	 *  @param[in] broker_addr kafka 地址 格式为ip1:port1,ip2:port2..
	 *  @param[in] topic kafka 主题
	 *  @param[in] offset 从kafka 哪里开始读取数据, KAFKA_EARLIEST_OFFSET 最开始的地方读取；KAFKA_LATEST_OFFSET 新添加消息的地方读取
	 *  @param[in] user_name 用户名
	 *  @param[in] password 密码
	 */
	bool InitKafka(const std::string& broker_addr, const std::string& topic, const std::string& group_id = "default",
	               Kafka_Offset offset = KAFKA_LATEST_OFFSET, const std::string& user_name = "", const std::string& password = "");

	/*
	 *  @brief 从kafka 中取数据, message 的二进制数据拷贝到string msg 中
	 *  @author zengkun
	 *  @param[out] msg 存放kafka 取出的rkmessage 数据
	 */
	bool ConsumerData(std::string& msg);

	// 获取kafka 远程链接的地址
	operator std::string();

private:
	rd_kafka_t* m_rk;
	rd_kafka_topic_partition_list_t* m_topics;

	std::string         m_kafka_addr;
};


#endif //KAFKA_KAFKA_CONSUMER_H
