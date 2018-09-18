import json
import paho.mqtt.client as mqtt
import time
import re
from translate_kafka import MqttTranslateKafka
import myconfig
from logs import log


class MQTTConsumer:
    """设备影子"""
    def __init__(self, topic):
        self.topic = topic
        self.client_id = time.strftime('%Y%m%d%H%M%S', time.gmtime())
        self.mqtt_client = mqtt.Client(self.client_id)
        self.mqtt_client.username_pw_set(myconfig.MQTT_USERNAME, myconfig.MQTT_PASSWORD)

    def on_mqtt_connect(self):
        """连接MQTT服务器"""
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(myconfig.MQTT_HOST, myconfig.MQTT_PORT, 60)
        self.mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        """订阅主题"""
        with open(myconfig.log_path + '/rule.log', "a+") as f:
            f.write('%s MQTT client...1' % time.strftime('%Y%m%d%H%M%S', time.gmtime()) + '\n')
        # 订阅topic
        client.subscribe(self.topic)

    @staticmethod
    def on_message(client, userdata, msg):
        """接收消息"""
        # 记录消费
        with open(myconfig.log_path + '/rule.log', "a+") as f:
            f.write('%s MQTT rule engine producer...1' % time.strftime('%Y%m%d%H%M%S', time.gmtime()) + '\n')

        try:
            mqtt_topic = msg.topic
            mqtt_data = msg.payload.decode('utf-8')
            data = json.loads(mqtt_data)
            topic_name = data["topic"]
            topic_name = topic_name.lower()
        except Exception as e:
            log.logging.error(e)
        else:
            war_ret = re.findall(r'warning', topic_name)
            error_ret = re.findall(r'error', topic_name)
            if war_ret or error_ret:
                with open(myconfig.log_path + '/warning_error_topic.log', "a+") as f:
                    f.write(data + '\n')
            else:
                mqtt_tra_kafka = MqttTranslateKafka()
                mqtt_tra_kafka.rule_engine_translate(mqtt_topic, data)


if __name__ == '__main__':
    # 多层通配符"#"
    # MQTT_topic = '/sys/${productKey}/${deviceName}/#'
    obj = MQTTConsumer(myconfig.MQTT_TOPIC)
    obj.on_mqtt_connect()
