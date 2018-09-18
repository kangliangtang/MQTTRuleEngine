import json
import paho.mqtt.publish as publish
import time


HOST = "172.16.16.16"
PORT = 1883
# PORT = 32178


def publish_func(topic, payload):
    """发布消息"""
    # 客户端ID（唯一）
    client_id = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    # 用户认证
    auth = {'username': 'admin', 'password': 'admin'}
    publish.single(topic=topic, payload=payload, hostname=HOST, port=PORT, client_id=client_id, auth=auth)


if __name__ == '__main__':
    # 发布主题
    update_topic = '/data/rule/queue'
    # 基础版数据格式
    payload = {
          "topic": "/6439387697231927821/dev20180829/property/post",
          "type": "data/warning/error",
          "id": "00001",
          "dataid": "{productKey}_{event}_{test001}",
          "app_params": [{
              "current": 0,
              "voltage": 115,
              "Status": "OK"
          }]
        }

    json_update_data = json.dumps(payload)
    publish_func(topic=update_topic, payload=json_update_data)
