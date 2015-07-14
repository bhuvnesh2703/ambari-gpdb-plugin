#!/usr/bin/env python

from resource_management import *
import pxf

class PxfService(Script):
  def install(self, env):
    self.install_packages(env)
    pxf.setup_user_group(env)
    pxf.generate_config_files(env)
    pxf.init(env)
    pxf.grant_permissions(env)

  def configure(self, env):
    import params
    env.set_params(params)
    pxf.setup_user_group(env)
    pxf.generate_config_files(env)
    pxf.grant_permissions(env)

  def start(self, env):
    self.configure(env)
    pxf.start(env)

  def stop(self, env):
    pxf.stop(env)

  def status(self, env):
    pxf.status(env)

if __name__ == "__main__":
  PxfService().execute()