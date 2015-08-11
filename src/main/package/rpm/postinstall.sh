#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def extract_os_family(os_tag):
  # os tag can be
  # <os family="redhat6">  <-- Ambari 2.0 or after
  # <os type="redhat6">    <-- prior to Ambari 2.0
  # See https://issues.apache.org/jira/browse/AMBARI-6270
  if 'type' in os_tag.attrib:
    return os_tag.attrib['type']

  if 'family' in os_tag.attrib:
    return os_tag.attrib['family']

  raise Exception("Neither 'family' nor 'tag' attribute is found in <os> tag in repoinfo.xml")

def add_gpdb_roles(stack_dir):
  file_path = '{0}/role_command_order.json'.format(stack_dir)
  json_data=open(file_path, 'r+')
  data = json.load(json_data)
  
  data['general_deps']['GPDBMASTER-START'] = ['GPDBSTANDBY-START']
  data['general_deps']['GPDB_SERVICE_CHECK-SERVICE_CHECK'] = ['GPDBMASTER-START']
  
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.truncate()
  json_data.close()

def add_pads_repo(stack_dir):
  file_path = '{0}/repos/repoinfo.xml'.format(stack_dir)
  PADS_REPO_NAME='GPDB'
  element_string = '<repo><baseurl>http://{0}/{1}</baseurl><repoid>{1}</repoid><reponame>{1}</reponame></repo>'.format(socket.getfqdn(), PADS_REPO_NAME)
  pads_element = ET.fromstring(element_string)
  tree = ET.parse(file_path)
  root = tree.getroot()

  file_needs_update = False
  for os_tag in root.findall('.//os'):
    os_family = extract_os_family(os_tag)
    if os_family in ['redhat6', 'suse11']:
      if not any(PADS_REPO_NAME in reponame.text for reponame in os_tag.findall('.//reponame')):
        os_tag.append(pads_element)
        file_needs_update = True

  if file_needs_update:
    tree.write(file_path)
  
PHD_STACK_DIR = '/var/lib/ambari-server/resources/stacks/PHD/3.0'
HDP_STACK_DIR = '/var/lib/ambari-server/resources/stacks/HDP/2.2'

if os.path.isdir(PHD_STACK_DIR):
  add_gpdb_roles(PHD_STACK_DIR)
  add_pads_repo(PHD_STACK_DIR)
elif os.path.isdir(HDP_STACK_DIR):
  add_gpdb_roles(HDP_STACK_DIR)
  add_pads_repo(HDP_STACK_DIR)
else:
  raise Exception("Neither PHD/3.0 nor HDP/2.2 stacks are found in /var/lib/ambari-server/resources/stacks/ directory")

EOT


