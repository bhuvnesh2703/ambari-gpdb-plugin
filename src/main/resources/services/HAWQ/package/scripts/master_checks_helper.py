#!/usr/bin/env python
from common import subprocess_command_with_results
import os

class HawqMaster(object):
  def __init__(self, hostname, datadir, user):
    self.hostname = hostname
    self.datadir = datadir
    self.user = user
    self.postmaster_opts_file = os.path.join(self.datadir, "postmaster.opts")

  def read_mtime_postmaster_opts(self):
    import params
    cmd = "stat -c %Y {0}".format(self.postmaster_opts_file)
    returncode, stdoutdata, stderrdata = subprocess_command_with_results(cmd, self.hostname)
    return stdoutdata

  def is_postmaster_opts_missing(self):
    import params
    cmd = "[ -f {0} ]".format(self.postmaster_opts_file)
    returncode, stdoutdata, stderrdata = subprocess_command_with_results(cmd, self.hostname)
    return not(returncode == 0)

  def read_postmaster_opts(self):
    import params
    cmd = "cat {0}".format(self.postmaster_opts_file)
    returncode, stdoutdata, stderrdata = subprocess_command_with_results(cmd, self.hostname)
    return self.convert_postmaster_content_to_list(stdoutdata)
    
  def is_datadir_existing(self):
    import params
    cmd = "[ -d {0} ]".format(self.datadir)
    returncode, stdoutdata, stderrdata = subprocess_command_with_results(cmd, self.hostname)
    return returncode == 0

  def convert_postmaster_content_to_list(self, postmaster_opts_content):
    return postmaster_opts_content.split()
  
  def get_standby_dbid(self):
    postmaster_opt_content = self.read_postmaster_opts()
    return postmaster_opt_content[postmaster_opt_content.index('"-x"') + 1]
