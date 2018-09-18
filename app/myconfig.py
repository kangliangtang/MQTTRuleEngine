import pymysql
import redis


# mysql基本配置
MYSQL_HOST = '172.16.16.134'
MYSQL_PORT = 3306
MYSQL_USER = 'beacon'
MYSQL_PASSWORD = 'Foxconn88!'
MYSQL_DB = 'db_3'

MYSQL_DB_CONN = pymysql.connect(
                        host=MYSQL_HOST,
                        port=MYSQL_PORT,
                        user=MYSQL_USER,
                        password=MYSQL_PASSWORD,
                        db=MYSQL_DB,
                        charset='utf8'
                        )

# 转发规则表
TRANSLATE_RULE = 'corepro_translate_rule'
# 行为表
TRANSLATE_RULE_ACTION = 'corepro_translate_rule_action'
# 自定义topic表
CUSTOMER_TOPIC = 'corepro_customer_topic'


# redis基本配置
REDIS_HOST = '172.16.16.56'
REDIS_PORT = 6379
REDIS_PASSWORD = 'foxconn168!'
REDIS_DB = 0
REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB)


# MQTT基本配置
MQTT_HOST = "172.16.16.16"
MQTT_PORT = 32178
MQTT_USERNAME = 'admin'
MQTT_PASSWORD = 'admin'
# 订阅的主题
MQTT_TOPIC = '/sys/corepro_rule'


# Kafka基本配置
BOOTSTRAP_SERVERS = ['172.18.0.25:9092', '172.18.0.47:9092', '172.18.0.44:9092']


log_path = '/var/log/rulelog'
