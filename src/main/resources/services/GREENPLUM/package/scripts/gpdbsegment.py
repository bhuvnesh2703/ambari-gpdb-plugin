#!/usr/bin/env python
from resource_management import *
import gpdb 

class GreenplumSegment(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    gpdb.common_setup(env)
    gpdb.segment_configure(env)
    gpdb.system_verification(env, "segment")

  def start(self, env):
    self.configure(env)
    self.display_help("start")

  def stop(self, env):
    self.display_help("stop")

  def status(self, env):
    import status_params
    for pid_gpdbsegment in status_params.pid_gpdbsegments:
      check_process_status(pid_gpdbsegment)

  def display_help(self, operation):
    Logger.info("Segment {0} operation is controlled by active greenplum database master, Please wait for master {0} operation to complete.".format(operation))

if __name__ == "__main__":
  GreenplumSegment().execute()
