import json
import paho.mqtt.publish as publish
import time


# HOST = "111.230.180.106"
# HOST = "134.175.158.2"
HOST = "119.29.123.174"
# HOST = "127.0.0.1"
PORT = 1883


def publish_func(topic, payload):
    """发布消息"""
    # 客户端ID（唯一）
    client_id = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    # 用户认证
    auth = {'username': 'admin', 'password': 'admin'}
    # auth = {'username': 'admin', 'password': 'password'}
    publish.single(topic=topic, payload=payload, hostname=HOST, port=PORT, client_id=client_id, auth=auth)


if __name__ == '__main__':
    # 发布主题
    update_topic = '/6439387697231927821/dev20180829/property/post'
    payload = {"id": 123456787, "params": {"current": 0, "voltage": 115, "Status": "OK"}}

    # loacl test
    # update_topic = '/sys/corepro_rule'
    # payload = {
    #       "topic": "/6439387697231927821/dev20180829/property/post",
    #       "type": "data/warning/error",
    #       "id": "00001",
    #       "dataid": "{productKey}_{event}_{test001}",
    #       "payload": {
    #             "productkey": "iot123456",
    #             "devicename": "iotdevice",
    #             "timestamp": "1534833831.497",
    #             "params": {
    #                   "current": 0,
    #                   "voltage": 115,
    #                   "Status": "OK"
    #                   }
    #             }
    #     }

    json_update_data = json.dumps(payload)
    publish_func(topic=update_topic, payload=json_update_data)
