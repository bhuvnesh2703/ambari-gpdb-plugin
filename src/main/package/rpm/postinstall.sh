#!/bin/bash

python <<EOT
import json
import os
from pprint import pprint

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0'):
  json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['HAWQ-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['HAWQMASTER-START'] = ['NAMENODE-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()
elif os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.1/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['HAWQ-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['HAWQMASTER-START'] = ['NAMENODE-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['HAWQ-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['HAWQMASTER-START'] = ['NAMENODE-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

EOT

