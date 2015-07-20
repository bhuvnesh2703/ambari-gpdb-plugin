import os
import subprocess

def subprocess_command_with_results(user, cmd, hostname):
  import params
  if params.hostname != hostname:
    cmd = "su - {0} -c 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {1} \"{2} \" '".format(user, hostname, cmd) 
  else: 
    cmd = "su - {0} -c '\"{1}\" '".format(user, cmd)
  Logger.info("user %s, cmd %s, hostname %s" % (user, cmd, hostname))
  process = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
  (stdoutdata, stderrdata) = process.communicate()
  Logger.info("process.returncode %s, stdoutdata %s, stderrdata %s" % (process.returncode, stdoutdata, stderrdata))
  return process.returncode, stdoutdata, stderrdata
