import os
import fnmatch
import params
from resource_management import *

master_dir = params.greenplum_master_dir
segments_dir = params.greenplum_data_dir
gpadmin_home = os.path.expanduser('~' + params.greenplum_user)
master_dir_file = "{0}/master-dir".format(gpadmin_home)
segments_dir_file = "{0}/segments-dir".format(gpadmin_home)
if os.path.isfile(master_dir_file):
  with open(master_dir_file, 'r') as f:
    master_dir = f.readline().strip()
if os.path.isfile(segments_dir_file):
  with open(segments_dir_file, 'r') as f:
    segments_dir = f.readline().strip()

cmd = "rm -rf {0}/*.pid".format(params.greenplum_tmp_dir)
Execute(cmd, timeout=600)

pid_gpdbmaster = "{0}/greenplum_master.pid".format(params.greenplum_tmp_dir)
pid_postmaster = "{0}/gpseg-1/postmaster.pid".format(master_dir)
with open("/tmp/pid_gpdbmaster", "w") as fh:
  fh.write("pid_gpdbmaster: " + str(pid_gpdbmaster) + " pid_postmaster: " + str(pid_postmaster))
if os.path.isfile(pid_postmaster):
  with open(pid_postmaster, 'r') as f:
    pid = f.readline()
    with open("/tmp/pidfile", "w") as fh:
      fh.write("pid: " + str(pid))
    File(pid_gpdbmaster,
         content=pid,
         owner=params.greenplum_user,
         group=params.greenplum_group)

segment_id = 0
for root, dirnames, filenames in os.walk(segments_dir):
  for filename in fnmatch.filter(filenames, 'postmaster.pid'):
    pid_postmaster_segment = os.path.join(root, filename)
    with open(pid_postmaster_segment, 'r') as f:
      pid = f.readline()

    pid_gpdbsegment = "{0}/greenplum_segment{1}.pid".format(params.greenplum_tmp_dir, segment_id)
    segment_id += 1

    File(pid_gpdbsegment,
         content=pid,
         owner=params.greenplum_user,
         group=params.greenplum_group)

pid_gpdbsegments = [os.path.join(params.greenplum_tmp_dir, x) for x in os.listdir(params.greenplum_tmp_dir) if 'greenplum_segment' in x]
if not pid_gpdbsegments:
  pid_gpdbsegments.append('dummy.pid')
