#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def updateRepoWithPads(repoinfoxml):
  pads_repo='PADS'
  pads_repo_str = '<repo><baseurl>http://' + socket.getfqdn() + '/' + pads_repo + '</baseurl><repoid>' + pads_repo + '</repoid><reponame>' + pads_repo + '</reponame></repo>'
  is_padsrepo_set = None

  tree = ET.parse(repoinfoxml)
  root = tree.getroot()

  for os_tag in root.findall('.//os'):
    if os_tag.attrib['type'] == 'redhat6' or os_tag.attrib['type'] == 'suse11':
      for reponame in os_tag.findall('.//reponame'):
        if 'PADS' in reponame.text:
          is_padsrepo_set = True
      if is_padsrepo_set is None:
        pads_element = ET.fromstring(pads_repo_str)
        os_tag.append(pads_element)
  if is_padsrepo_set is None:
    tree.write(repoinfoxml)

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['HAWQ-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['HAWQMASTER-START'] = ['NAMENODE-START','DATANODE-START','HAWQSTANDBY-START']
  data['general_deps']['HAWQ_SERVICE_CHECK-SERVICE_CHECK'] = ['HAWQMASTER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()
elif os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['HAWQ-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['HAWQMASTER-START'] = ['NAMENODE-START','DATANODE-START','HAWQSTANDBY-START']
  data['general_deps']['HAWQ_SERVICE_CHECK-SERVICE_CHECK'] = ['HAWQMASTER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close() 

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml'):
  updateRepoWithPads('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml')
elif os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml'):
  updateRepoWithPads('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml')

EOT


