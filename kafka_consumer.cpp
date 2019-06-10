//
// Created by root on 18-11-4.
//
#include <string.h>

#include <glog/logging.h>
#include "kafka_consumer.h"


#define HANDLER_CONF_ERROR(conf, key, value, errstr) \
    if (rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) \
    { \
        LOG(ERROR) << "consumer set kafka conf fail, " << " key = " << key << " value = " << value << errstr; \
        return false; \
    } \

#define HANDLER_TOPIC_CONF_ERROR(conf, key, value, errstr) \
    if (rd_kafka_topic_conf_set(conf, key, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) { \
        LOG(ERROR) << "consumer set kafka topic conf fail, " << " key = " << key << " value = " << value << errstr; \
        return false; \
    } \

#define CONSUMER_QUEUE_LENGTH "100000" //

Kafka_Consumer::Kafka_Consumer()
{
    m_rk = nullptr;
    m_topics = nullptr;
}

bool Kafka_Consumer::InitKafka(const std::string& broker_addr, const std::string& topic, const std::string& group_id,
                               Kafka_Offset offset, const std::string& user_name, const std::string& password)
{
    char errstr[256] = {0};

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    HANDLER_CONF_ERROR(conf, "group.id", group_id.c_str(), errstr);
    HANDLER_CONF_ERROR(conf, "queued.min.messages", CONSUMER_QUEUE_LENGTH, errstr);

    if (! user_name.empty() && ! password.empty()) {
        HANDLER_CONF_ERROR(conf, "security.protocol", "sasl_plaintext", errstr);
        HANDLER_CONF_ERROR(conf, "sasl.mechanisms", "PLAIN", errstr);
        HANDLER_CONF_ERROR(conf, "sasl.username", user_name.c_str(), errstr);
        HANDLER_CONF_ERROR(conf, "sasl.password", password.c_str(), errstr);
    }

    rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();
    HANDLER_TOPIC_CONF_ERROR(topic_conf, "offset.store.method", "broker", errstr);

    rd_kafka_conf_set_default_topic_conf(conf, topic_conf);
    if (offset == KAFKA_EARLIEST_OFFSET) {
        HANDLER_TOPIC_CONF_ERROR(topic_conf, "auto.offset.reset", "earliest", errstr);
    } else {
        HANDLER_TOPIC_CONF_ERROR(topic_conf, "auto.offset.reset", "latest", errstr);
    }

    m_rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (! m_rk) {
        LOG(ERROR) << errstr;
        return false;
    }

    int count = rd_kafka_brokers_add(m_rk, broker_addr.c_str());
    if (count <= 0)
        return false;

    rd_kafka_resp_err_t err;
    rd_kafka_poll_set_consumer(m_rk);
    m_topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(m_topics, topic.c_str(), RD_KAFKA_PARTITION_UA);
    if (err = rd_kafka_subscribe(m_rk, m_topics)) {
        LOG(ERROR) << errstr;
        return false;
    }
    m_kafka_addr = broker_addr;
    return true;
}

bool Kafka_Consumer::ConsumerData(std::string& msg)
{
    rd_kafka_message_t *rkmessage = nullptr;
    rkmessage = rd_kafka_consumer_poll(m_rk, 1000);
    if (! rkmessage)
        return false;

    if (rkmessage->err) {
        if (rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
        {
            LOG(ERROR) << "kafka consumer error = " << rd_kafka_message_errstr(rkmessage);
        }
        rd_kafka_message_destroy(rkmessage);
        return false;
    }

    msg.resize(rkmessage->len);
    memcpy(&*msg.begin(), rkmessage->payload, rkmessage->len);

    rd_kafka_message_destroy(rkmessage);
    return true;
}

Kafka_Consumer::~Kafka_Consumer() {
    rd_kafka_resp_err_t err = rd_kafka_consumer_close(m_rk);
    rd_kafka_topic_partition_list_destroy(m_topics); //destroy kafka handle
    rd_kafka_destroy(m_rk);

    int run = 5; //等待所有rd_kafka_t对象销毁，所有kafka对象被销毁，返回0，超时返回-1
    while (run-- > 0 && rd_kafka_wait_destroyed(1000) == -1) {
        LOG(INFO) << "Waiting for librdkafka to decommission\n";
    }
    if (run <= 0) {
        rd_kafka_dump(stdout, m_rk);
    }
}

Kafka_Consumer::operator std::string()
{
    return m_kafka_addr;
}
