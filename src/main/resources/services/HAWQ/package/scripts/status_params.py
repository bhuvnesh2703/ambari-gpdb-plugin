import os
import fnmatch
import params
from resource_management import *

master_dir = params.hawq_master_dir
segments_dir = params.hawq_data_dir
gpadmin_home = os.path.expanduser('~' + params.hawq_user)
master_dir_file = "{0}/master-dir".format(gpadmin_home)
segments_dir_file = "{0}/segments-dir".format(gpadmin_home)
if os.path.isfile(master_dir_file):
  with open(master_dir_file, 'r') as f:
    master_dir = f.readline().strip()
if os.path.isfile(segments_dir_file):
  with open(segments_dir_file, 'r') as f:
    segments_dir = f.readline().strip()

cmd = "rm -rf {0}/*.pid".format(params.hawq_tmp_dir)
Execute(cmd, timeout=600)

pid_hawqmaster = "{0}/hawq_master.pid".format(params.hawq_tmp_dir)
pid_postmaster = "{0}/gpseg-1/postmaster.pid".format(master_dir)
if os.path.isfile(pid_postmaster):
  with open(pid_postmaster, 'r') as f:
    pid = f.readline()
    File(pid_hawqmaster,
         content=pid,
         owner=params.hawq_user,
         group=params.hawq_group)

segment_id = 0
for root, dirnames, filenames in os.walk(segments_dir):
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
