import os
import subprocess
import re
from resource_management.core.logger import Logger

def subprocess_command_with_results(cmd, remote_hostname=None):
  if remote_hostname:
    cmd = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {0} \" {1} \"".format(remote_hostname, cmd)
  process = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
  (stdoutdata, stderrdata) = process.communicate()
  return process.returncode, stdoutdata, stderrdata

def get_processes_on_port(port):
  # Identify processes running on input port
  cmd = "netstat -tupln | egrep ':{0}'".format(port)
  (retcode, out, err) = subprocess_command_with_results(cmd, remote_hostname=None)
  if err:
    raise Exception("Execution of command `%s` failed. Error returned is \n%s" % (cmd, err))
  # Strip multiple spaces in the output to single space, and remove the newline character from output
  out = re.sub(" +", " ", out).strip()
  Logger.info("List of process running on port %d:\n%s" % (port, out.split('\n') if len(out) > 0 else 'None' ))
  return out
