#ifndef _SN_KAFKA_H_
#define _SN_KAFKA_H_

#include <rdkafka.h>
#include <string>

class Kafka_Producer
{
public:
	Kafka_Producer(void);
	~Kafka_Producer(void);

	/*
	 *  @brief 指定地址和topic初始化链接kafka, 也可以基于sasl_plaintext 协议, 加入用户名和密码链接; 如果用户名, 密码为空, 表示不用验证登录
	 *  @author zengkun
	 *  @param[in] addr kafka 地址 格式为ip1:port1,ip2:port2..
	 *  @param[in] topic kafka 主题
	 *  @param[in] user_name 用户名
	 *  @param[in] password 密码
	 */
	bool Init(const std::string& addr, const std::string& topic, const std::string& user_name = "",
	          const std::string& password = "");

	/*
	 *  @brief 向kafka 上面发送数据, 可以基于分区id 来发, 也可以基于key来确定分区号发送数据
	 *  设置了 partition 就发到指定的分区号上面; partition = -1(没有选择分区id), 设置了key, 向hash(key) % kafka 分区 发送数据; 两个都没设置，就随机选择1个分区发送数据
	 *  @author zengkun
	 *  @param[in] data 发送的二进制数据
	 *  @param[in] datalen 数据长度
	 *  @param[in] partition 分区id
	 *  @param[in] key 发送数据的key, partition id = hash(key) % kafka 分区数
	 *  @Return: 0 success, others fail
	 */
	int SendData(const void* data, int datalen, int partition = RD_KAFKA_PARTITION_UA, const std::string& key = "");

	// 获取kafka 远程链接的地址
	operator std::string();

private:
	rd_kafka_t*         m_producer;
	rd_kafka_topic_t*   m_topic;

	int                 m_errcode;

	std::string         m_kafka_addr;
};

#endif
