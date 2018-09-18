import json
import re
import time
import myconfig
from logs import log
import jpype
import os.path
from kafka import KafkaProducer


class MqttTranslateKafka:
    """MQTT转发Kafka"""

    @staticmethod
    def get_redis_rule(topic):
        """从redis获取规则条件
        :param topic： kafka的生产主题
        :return rule_list：  字段，规则条件，行为
        """
        redis_key_list = None
        try:
            r = myconfig.REDIS_CLIENT
            redis_key_list = r.keys(topic + '_*')
        except Exception as e:
            log.logging.error("Redis key Error: %s" % e + '\n')

        rule_list = []
        if redis_key_list:
            field = None
            rule_condition = None
            actions = None
            for redis_key in redis_key_list:
                try:
                    redis_value = myconfig.REDIS_CLIENT.hget(redis_key, 'rule')
                    redis_value = json.loads(redis_value.decode('utf-8'))
                    field = redis_value["field"]
                    rule_condition = redis_value["rule_condition"]
                    actions = redis_value["actions"]
                except Exception as e:
                    log.logging.error("Redis Error: %s" % e + '\n')
                else:
                    rule_condition = rule_condition
                    actions = actions

                if actions:
                    rule_dict = {"field": field, "rule_condition": rule_condition, "actions": actions}
                    rule_list.append(rule_dict)
                else:
                    log.logging.error("Redis key actions Error" + '\n')
        else:
            rule_list = rule_list
        return rule_list

    @staticmethod
    def get_mysql_rule(topic):
        """从mysql获取规则条件,并存入redis缓存中
        :param topic： kafka的生产主题
        :return rule_list：  字段，规则条件，行为
        """
        # 一个topic对应多个trh_id, 一个trh_id对应多个actions
        cursor = myconfig.MYSQL_DB_CONN.cursor()
        # 1.从mysql 获取rule_list
        rule_list = []
        try:
            # 1.1 通过topic,从"转发规则表"查所有trh_id
            corepro_translate_rule = myconfig.TRANSLATE_RULE
            trh_id_sql = "select trh_id from %s where topic='%s';" % (corepro_translate_rule, topic)
            cursor.execute(trh_id_sql)
            trh_id_all = cursor.fetchall()
            if trh_id_all:
                for trh_id in trh_id_all:
                    # 存储field, rule_condition, actions
                    rule_dict = dict()
                    # 1.2 通过trh_id,从"转发规则表"获取field , rule_condition
                    corepro_translate_rule = myconfig.TRANSLATE_RULE
                    field_condition_sql = "select field,rule_condition from %s where trh_id='%s';" % (corepro_translate_rule, trh_id)
                    cursor.execute(field_condition_sql)
                    field_condition = cursor.fetchone()
                    field = field_condition[0]
                    rule_condition = field_condition[1]

                    # 1.3 通过trh_id,从"行为表"获取action_type, action, relation_id
                    corepro_translate_rule_action = myconfig.TRANSLATE_RULE_ACTION
                    type_sql = "select action_type,action,relation_id from %s where trh_id='%s'" % (corepro_translate_rule_action, trh_id)
                    cursor.execute(type_sql)
                    actions_rel_id_all = cursor.fetchall()

                    # 存储所有的action_type, action
                    actions = []
                    for actions_rel_id in actions_rel_id_all:
                        action_type = actions_rel_id[0]
                        action = actions_rel_id[1]
                        relation_id = actions_rel_id[2]
                        actions_dict = dict()
                        # actions: [{action_type: 1, action: topicwww}]}
                        if action:
                            action = action
                        else:
                            # 通过relation_id 关联查询topic_name
                            # corepro_customer_topic
                            corepro_customer_topic = myconfig.CUSTOMER_TOPIC
                            topic_name_sql = "select topic_name from %s where relation_id='%s'" % (corepro_customer_topic, relation_id)
                            cursor.execute(topic_name_sql)
                            topic_name = cursor.fetchone()
                            action = topic_name
                        actions_dict['action_type'] = action_type
                        actions_dict['action'] = action
                        actions.append(actions_dict)

                    rule_dict["field"] = field
                    rule_dict["rule_condition"] = rule_condition
                    rule_dict["actions"] = actions
                    rule_list.append(rule_dict)

                    # 2.设置redis缓存
                    r = myconfig.REDIS_CLIENT
                    redis_key = topic + '_' + trh_id
                    rule_val = json.dumps(rule_dict)
                    ret = r.hset(redis_key, 'rule', rule_val)
                    # 记录日志
                    with open(myconfig.log_path + '/rule.log', "a+") as f:
                        f.write('%s Set redis cache %d' % (time.strftime('%Y%m%d%H%M%S', time.gmtime()), ret) + '\n')
        except Exception as e:
            log.logging.error(e)
        return rule_list

    @staticmethod
    def get_params(mqtt_topic, data):
        """获取mqtt message 中的params
        :param mqtt_topic: 主题
        :param data: mqtt消息数据
        :return params: type is dict
        """
        params = None
        if mqtt_topic == '/sys/corepro_rule':
            payload = data["payload"]
            params = payload["params"]
        elif mqtt_topic == '/data/rule/queue':
            params = data["app_params"]
            params = params[0]
        else:
            pass
        return params

    @staticmethod
    def condition_check(params, condition):
        """ 按照条件筛选出符合的字段
        :param params: mqtt订阅到的消息数据中提取param
        :param condition: 规则条件
        :return: check_ret  检查后的结果 True or False
        """
        if params:
            # jar包的存放路径
            # jar_path = os.path.join('/home/docker/code/app/', 'QLExpress-3.2.0-jar-with-dependencies.jar')
            jar_path = os.path.join(os.path.dirname(__file__), 'QLExpress-3.2.0-jar-with-dependencies.jar')

            # JVM默认路径
            jvm_path = jpype.getDefaultJVMPath()
            # 开启JVM
            if not jpype.isJVMStarted():
                jpype.startJVM(jvm_path, "-ea", "-Djava.class.path=%s" % jar_path)
            # 创建实例
            java_class = jpype.JClass('com.ql.util.invoke.ExecuteSql')
            java_class = java_class()
            # 调用方法
            para_str = json.dumps(params)
            condition = condition.replace('AND', 'and').replace('OR', 'or')
            with open(myconfig.log_path + '/rule.log', "a+") as f:
                f.write('%s JAVA transfer...1' % time.strftime('%Y%m%d%H%M%S', time.gmtime()) + '\n')

            check_ret = False
            try:
                check_ret = java_class.executeSqlCondition(condition, para_str)
            except Exception as e:
                log.logging.error(e)
            else:
                check_ret = check_ret
            finally:
                # 关闭JVM
                jpype.shutdownJVM()
        else:
            check_ret = False

        with open(myconfig.log_path + '/rule.log', "a+") as f:
            f.write('%s JAVA transfer result: %s' % (time.strftime('%Y%m%d%H%M%S', time.gmtime()), check_ret) + '\n')

        return check_ret

    @staticmethod
    def get_field(field, params_keys):
        """判断filed是否全部存在于params中"""
        if field:
            field_list = re.findall(r'[a-zA-Z0-9]+', field, re.S)
            res = list(set(field_list).intersection(set(params_keys)))
            # 判断filed的key是否全部存在于params中
            if len(res) == len(field_list):
                field_list = field_list
            else:
                field_list = None
        else:
            field_list = params_keys
        return field_list

    def field_data_process(self, mqtt_topic, field, data):
        """ 根据筛选字段对转发数据处理
        :param mqtt_topic: mqtt消费主题
        :param field: 筛选字段名 "col1, col2"
        :param data:  mqtt订阅的消息数据 type is dict
        :return: field_data: 根据field处理后转发Kafka的消息数据
        """
        new_params = dict()
        field_data = None
        if mqtt_topic == '/sys/corepro_rule':
            payload = data["payload"]
            params = payload["params"]
            params_keys = params.keys()
            field_list = self.get_field(field, params_keys)
            if field_list:
                for key in field_list:
                    new_params[key] = params[key]
                payload["params"] = new_params
                field_data = data
            else:
                field_data = None
        elif mqtt_topic == '/data/rule/queue':
            app_params = data["app_params"]
            new_app_params = []
            for params in app_params:
                params_keys = params.keys()
                field_list = self.get_field(field, params_keys)
                if field_list:
                    for key in field_list:
                        new_params[key] = params[key]
                    new_app_params.append(new_params)
                    field_data = new_app_params
                else:
                    field_data = None
        else:
            pass
        return field_data

    def translate_topic(self, topic, condition, mqtt_topic, field, data):
        """转发topic"""
        # 直接从报文获取topic_name
        topic_name = topic
        if topic_name is None:
            topic_name = data['dataid']

        kafka_data = None
        # a. condition不为空
        if condition:
            # 条件筛选
            check_ret = None
            try:
                params = self.get_params(mqtt_topic, data)
                check_ret = self.condition_check(params, condition)
            except Exception as e:
                log.logging.error(e)
            if check_ret:
                # 根据field字段对转发数据处理
                field_data = self.field_data_process(mqtt_topic=mqtt_topic, field=field, data=data)
                if field_data:
                    kafka_data = json.dumps(field_data)
                else:
                    log.logging.error("field error" + '\n')
            else:
                return
        # b. condition为空
        else:
            # 将mqtt consumer接收到的所有数据直接转发
            kafka_data = json.dumps(data)

        # 调用kafka
        with open(myconfig.log_path + '/rule.log', "a+") as f:
            f.write('%s Kafka transfer...1' % time.strftime('%Y%m%d%H%M%S', time.gmtime()) + '\n')
        try:
            kafka_producer = KafkaRuleEngine()
            kafka_producer.producer_func(topic=topic_name, payload=kafka_data)
        except Exception as e:
            log.logging.error("Kafka transfer error: %s" % e + '\n')

    def rule_engine_translate(self, mqtt_topic, data):
        """将mqttd订阅消息进行条件筛选，转发Kafka"""
        if 'topic' in data.keys():
            topic = data["topic"]
            # 1.获取rule_list  (field & condition & actions)
            rule_list = self.get_redis_rule(topic)
            if not rule_list:
                rule_list = self.get_mysql_rule(topic)

            if rule_list:
                for field_condition_actions in rule_list:
                    field = field_condition_actions["field"]
                    rule_condition = field_condition_actions["rule_condition"]
                    actions = field_condition_actions["actions"]
                    # 2.根据action_type做不同的转发(1.转发topic  2.转发第三方服务)
                    for rule_action in actions:
                        action_type = rule_action["action_type"]
                        topic_name = rule_action['action']
                        # 2.1 转发topic
                        if action_type == 1:
                            self.translate_topic(topic_name, rule_condition, mqtt_topic, field, data)
                        # 2.2 转发第三方服务
                        elif action_type == 2:
                            pass
                        else:
                            log.logging.error("action_type error" + '\n')
            else:
                log.logging.error("This topic rule is empty!" + '\n')
        else:
            log.logging.error("MQTT message 'topic' field does not exist!" + '\n')


class KafkaRuleEngine:
    """Kafka Producer"""
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=myconfig.BOOTSTRAP_SERVERS)

    def producer_func(self, topic, payload):
        """
        :param topic:  写入kafka的主题
        :param payload: 写入数据
        :return:  无
        """
        # 记录
        with open(myconfig.log_path + '/rule.log', "a+") as f:
            f.write('%s Kafka rule engine producer...1' % time.strftime('%Y%m%d%H%M%S', time.gmtime()) + '\n')

        self.producer.send(topic, payload.encode())
        self.producer.close()
