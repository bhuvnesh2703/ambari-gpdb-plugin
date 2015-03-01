#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def updateRepoWithPads(repoinfoxml):
  is_padsrepo_set = None

  tree = ET.parse(repoinfoxml)
  root = tree.getroot()

  for os_tag in root.findall('.//os'):
    if os_tag.attrib['type'] == 'redhat6':
      for repo in os_tag.findall('.//repo'):
        for reponame in repo.findall('.//reponame'):
          if 'PADS' in reponame.text:
            is_padsrepo_set = True
            os_tag.remove(repo)
  if is_padsrepo_set == True:
    tree.write(repoinfoxml)

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('HAWQ-INSTALL'):
    data['general_deps'].pop('HAWQ-INSTALL')
  if data['general_deps'].has_key('HAWQMASTER-START'):
    data['general_deps'].pop('HAWQMASTER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()
elif os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('HAWQ-INSTALL'):
    data['general_deps'].pop('HAWQ-INSTALL')
  if data['general_deps'].has_key('HAWQMASTER-START'):
    data['general_deps'].pop('HAWQMASTER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml'):
  updateRepoWithPads('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml')
elif os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml'):
  updateRepoWithPads('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml')

EOT

