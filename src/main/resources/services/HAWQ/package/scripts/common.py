import os
import subprocess
from resource_management import *

def subprocess_command_with_results(cmd, remote_hostname=None):
  if remote_hostname:
    cmd = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {0} \"{1} \"".format(remote_hostname, cmd)
  process = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
  (stdoutdata, stderrdata) = process.communicate()
  return process.returncode, stdoutdata, stderrdata
