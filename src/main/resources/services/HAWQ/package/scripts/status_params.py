"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
import fnmatch
import params
from resource_management import *

pid_hawqmaster = "{0}/hawq_master.pid".format(params.hawq_tmp_dir)
pid_postmaster = "{0}/gpseg-1/postmaster.pid".format(params.hawq_master_dir)
if os.path.isfile(pid_postmaster):
  with open(pid_postmaster, 'r') as f:
    pid = f.readline()
    File(pid_hawqmaster,
         content=pid,
         owner=params.hawq_user,
         group=params.hawq_group)

segment_id = 0
for root, dirnames, filenames in os.walk(params.hawq_data_dir):
  for filename in fnmatch.filter(filenames, 'postmaster.pid'):
    pid_postmaster_segment = os.path.join(root, filename)
    with open(pid_postmaster_segment, 'r') as f:
      pid = f.readline()

    pid_hawqsegment = "{0}/hawq_segment{1}.pid".format(params.hawq_tmp_dir, segment_id)
    segment_id += 1

    File(pid_hawqsegment,
         content=pid,
         owner=params.hawq_user,
         group=params.hawq_group)

pid_hawqsegments = [os.path.join(params.hawq_tmp_dir, x) for x in os.listdir(params.hawq_tmp_dir) if 'hawq_segment' in x]
if not pid_hawqsegments:
  pid_hawqsegments.append('dummy.pid')
