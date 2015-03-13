#!/usr/bin/env python

'''
Adapted from
ambari/ambari-agent/src/main/python/ambari_agent/Hardware.py
'''

import os.path
import logging
import subprocess
from Facter import Facter

logger = logging.getLogger()

class Hardware:
  SSH_KEY_PATTERN = 'ssh.*key'

  def __init__(self):
    self.hardware = {}
    self.hardware['mounts']      = self.osdisks()
    self.hardware['mounts_hawq'] = self.osdisks_hawq()
    self.hardware['osparams']    = self.osparams()
    self.hardware['osversion']   = self.osversion()
    self.hardware['user_groups'] = self.groups()
    
    otherInfo = Facter().facterInfo()
    self.hardware.update(otherInfo)
    pass

  @staticmethod
  def extractMountInfo(outputLine):
    if outputLine == None or len(outputLine) == 0:
      return None

      """ this ignores any spaces in the filesystemname and mounts """
    split = outputLine.split()
    if (len(split)) == 7:
      device, type, size, used, available, percent, mountpoint = split
      mountinfo = {
        'size': size,
        'used': used,
        'available': available,
        'percent': percent,
        'mountpoint': mountpoint,
        'type': type,
        'device': device}
      return mountinfo
    else:
      return None

  @staticmethod
  def osdisks():
    """ Run df to find out the disks on the host. Only works on linux 
    platforms. Note that this parser ignores any filesystems with spaces 
    and any mounts with spaces. """
    mounts = []
    df = subprocess.Popen(["df", "-kPT"], stdout=subprocess.PIPE)
    dfdata = df.communicate()[0]
    lines = dfdata.splitlines()
    for l in lines:
      mountinfo = Hardware.extractMountInfo(l)
      if mountinfo != None and os.access(mountinfo['mountpoint'], os.W_OK):
        mounts.append(mountinfo)
      pass
    pass
    return mounts

  ############## HAWQ (BEGIN) ############## 
  @staticmethod
  def osdisks_hawq():
    """ Run df to find out the disks on the host for HAWQ """
    import params
    mounts = {}
    if os.path.isdir(params.hawq_master_dir):
      mounts["master"]  = Hardware.osdisks_directory(params.hawq_master_dir)
    if os.path.isdir(params.hawq_data_dir):
      mounts["segment"] = Hardware.osdisks_directory(params.hawq_data_dir)
    return mounts

  @staticmethod
  def osdisks_directory(directory):
    mounts = []
    df = subprocess.Popen(["df", "-kPT", directory], stdout=subprocess.PIPE)
    dfdata = df.communicate()[0]
    lines = dfdata.splitlines()
    for l in lines:
      mountinfo = Hardware.extractMountInfo(l)
      if mountinfo != None and os.access(mountinfo['mountpoint'], os.W_OK):
        mounts.append(mountinfo)
      pass
    pass
    # Only check the mounting of one directory, 
    # safe to assume always one line returned
    return mounts[0]

  @staticmethod
  def osparams():
    osparams = {}
    sysctl = subprocess.Popen(["sysctl", "-a"], stdout=subprocess.PIPE)
    sysctldata = sysctl.communicate()[0]
    lines = sysctldata.splitlines()
    for l in lines:
      param, val = l.split(" = ")
      osparams[param] = val
    return osparams

  @staticmethod
  def osversion():
    version = subprocess.Popen("cat /etc/redhat-release", shell=True, stdout=subprocess.PIPE)
    versiondata = version.communicate()[0]
    if not versiondata:
      return ''
    return versiondata.split(' ')[2]

  @staticmethod
  def groups():
    import params
    groups = subprocess.Popen("groups %s" % params.hawq_user, shell=True, stdout=subprocess.PIPE)
    groupsdata = groups.communicate()[0]
    return groupsdata.split(':')[1].split()
  ############## HAWQ (END) ################


  def get(self):
    return self.hardware

def main(argv=None):
  hardware = Hardware()
  print hardware.get()

if __name__ == '__main__':
  main()
