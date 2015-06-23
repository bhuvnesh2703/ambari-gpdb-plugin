#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def extractOsFamily(os_tag):
  # os tag can be
  # <os family="redhat6">  <-- Ambari 2.0 or after
  # <os type="redhat6">  <-- prior to Ambari 2.0
  # See https://issues.apache.org/jira/browse/AMBARI-6270
  if 'type' in os_tag.attrib:
    return os_tag.attrib['type']

  if 'family' in os_tag.attrib:
    return os_tag.attrib['family']

  raise Exception("Neither 'family' nor 'tag' attribute is found in <os> tag in " + repoinfoxml)


def updateRepoWithPads(repoinfoxml):
  PADS_REPO_NAME='PADS'
  pads_element = ET.fromstring('<repo><baseurl>http://' + socket.getfqdn() + '/' + PADS_REPO_NAME + '</baseurl><repoid>' + PADS_REPO_NAME + '</repoid><reponame>' + PADS_REPO_NAME + '</reponame></repo>')
  is_padsrepo_modified = False

  tree = ET.parse(repoinfoxml)
  root = tree.getroot()

  for os_tag in root.findall('.//os'):
    os_family = extractOsFamily(os_tag)

    if os_family in ['redhat6', 'suse11']:
      is_pads_found = False
      for reponame in os_tag.findall('.//reponame'):
        if PADS_REPO_NAME in reponame.text:
          is_pads_found = True
          break

      if not is_pads_found:
        os_tag.append(pads_element)
        is_padsrepo_modified = True

  if is_padsrepo_modified:
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


