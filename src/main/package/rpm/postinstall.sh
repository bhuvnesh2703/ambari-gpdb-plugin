#!/bin/bash

python <<EOT
import json
from pprint import pprint
json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.1/role_command_order.json', 'r+')
data = json.load(json_data)
data['general_deps']['HAWQ-INSTALL'] = "[HDFS-INSTALL]"
data['general_deps']['HAWQMASTER-START'] = "[NAMENODE-START]"
json_data.seek(0)
json.dump(data, json_data, indent=2)
json_data.close()
json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
data = json.load(json_data)
data['general_deps']['HAWQ-INSTALL'] = "[HDFS-INSTALL]"
data['general_deps']['HAWQMASTER-START'] = "[NAMENODE-START]"
json_data.seek(0)
json.dump(data, json_data, indent=2)
json_data.close()
EOT


