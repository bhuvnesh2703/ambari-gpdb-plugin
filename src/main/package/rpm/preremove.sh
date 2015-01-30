#!/bin/bash

python <<EOT
import json
from pprint import pprint
json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0.0.0/role_command_order.json', 'r+')
data = json.load(json_data)
json_data.close()
EOT


