
#include "kafka_producer.h"
#include <glog/logging.h>

#define HANDLER_CONF_ERROR(conf, key, value, errstr) \
    if (rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) \
	{ \
	    LOG(ERROR) << "producer set kafka conf fail, " << " key = " << key << " value = " << value << errstr; \
		return false; \
	} \

#define PRODUCER_QUEUE_LENGTH "100000" // 发送队列缓存区长度

Kafka_Producer::Kafka_Producer()
{
	m_producer = nullptr;
	m_topic = nullptr;
	m_errcode = RD_KAFKA_RESP_ERR_UNKNOWN;
}

Kafka_Producer::~Kafka_Producer()
{
	rd_kafka_flush(m_producer, 1000);

	if(m_topic) {
		rd_kafka_topic_destroy(m_topic);
		m_topic = NULL;
	}
	if(m_producer) {
		rd_kafka_destroy(m_producer);
		m_producer = NULL;
	}
}

void Msg_Delivered(rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t error_code, void *opaque, void *msg_opaque)
{
	if(msg_opaque != NULL) {
		rd_kafka_resp_err_t *status = (rd_kafka_resp_err_t*)msg_opaque;
		*status = error_code;
	}
}

bool Kafka_Producer::Init(const std::string& addr, const std::string& topic, const std::string& user_name,
                          const std::string& password)
{
	rd_kafka_conf_t *rk_conf = rd_kafka_conf_new();
	rd_kafka_conf_set_dr_cb(rk_conf, Msg_Delivered);
	char errstr[256] = {0};

	HANDLER_CONF_ERROR(rk_conf, "queued.min.messages", PRODUCER_QUEUE_LENGTH, errstr);
	if (! user_name.empty() && ! password.empty()) {
		HANDLER_CONF_ERROR(rk_conf, "security.protocol", "sasl_plaintext", errstr)
		HANDLER_CONF_ERROR(rk_conf, "sasl.mechanisms", "PLAIN", errstr)
		HANDLER_CONF_ERROR(rk_conf, "sasl.username", user_name.c_str(), errstr)
		HANDLER_CONF_ERROR(rk_conf, "sasl.password", password.c_str(), errstr)
	}

	m_producer = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr));
	if (!m_producer) {
		LOG(ERROR) << "rd_kafka_new producer fail,  " << errstr;
		return false;
	}

	int err = rd_kafka_brokers_add(m_producer, addr.c_str());
	if (err == 0) {
		LOG(ERROR) << "rd_kafka_brokers_add fail, "  << errstr;
		return false;
	}

	m_topic = rd_kafka_topic_new(m_producer, topic.c_str(), nullptr);
	if (!m_topic) {
		LOG(ERROR) << "rd_kafka_topic_new fail, "  << errstr;
		return false;
	}

	m_kafka_addr = addr;
	return true;
}

int Kafka_Producer::SendData(const void* data, int datalen, int partition, const std::string& key)
{
#define TIMEOUT_MS 10000

	m_errcode = RD_KAFKA_RESP_ERR_UNKNOWN;
	int err = rd_kafka_produce(m_topic, partition, RD_KAFKA_MSG_F_COPY, const_cast<void*>(data), datalen, key.c_str(), key.length(), &m_errcode);

	if (err != -1) {
		rd_kafka_poll(m_producer, TIMEOUT_MS);
	}

	return m_errcode;
}

Kafka_Producer::operator std::string()
{
	return m_kafka_addr;
}
