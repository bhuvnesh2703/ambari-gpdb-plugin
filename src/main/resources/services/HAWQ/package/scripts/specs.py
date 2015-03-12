"""
Requirements gathered from
http://pivotalhd.docs.pivotal.io/doc/2100/webhelp/index.html#topics/PreparingtoInstallHAWQ.html
http://pivotalhd.docs.pivotal.io/doc/2100/webhelp/index.html#topics/InstallingHAWQ.html
"""

requirements = {
  "minimum_memory" : {
    "value"   : 16*1024*1024, # value in KB
    "message" :"Memory requirements: 16 GB RAM per server. And recommended memory on production cluster is 128 GB."
  },
  "minimum_disk" : {
    "value"   : 2*1024*1024, # value in KB
    "message" : "Disk requirements: 2GB per host for HAWQ installation"
  },
  "minimum_version" : {
    "value"   : "6.4",
    "message" : "OS version requirements: RedHat/Centos 6.4 or above"
  },
  "max_segments_per_node" : {
    "value"   : 10,
    "message" : "Number of segment processes per node cannot be greater than 10"
  },
}

# RHEL 6.x platforms
osparams_string = """
kernel.shmmax = 500000000
kernel.shmmni = 4096
kernel.shmall = 4000000000
kernel.sem = 250 512000 100 2048
kernel.sysrq = 1
kernel.core_uses_pid = 1
kernel.msgmnb = 65536
kernel.msgmax = 65536
kernel.msgmni = 2048
net.ipv4.tcp_syncookies = 0 
net.ipv4.ip_forward = 0 
net.ipv4.conf.default.accept_source_route = 0 
net.ipv4.tcp_tw_recycle = 1 
net.ipv4.tcp_max_syn_backlog = 200000 
net.ipv4.conf.all.arp_filter = 1 
net.ipv4.ip_local_port_range = 1025 65535 
net.core.netdev_max_backlog = 200000 
fs.nr_open = 3000000
kernel.threads-max = 798720
kernel.pid_max = 798720
# increase network
net.core.rmem_max=2097152
net.core.wmem_max=2097152
vm.overcommit_memory = 2
"""

osparams = {}
for line in osparams_string.splitlines():
  if line == '' or line[0] == '#': continue
  param, val = [x.strip() for x in line.split("=")]
  osparams[param] = { 
    "value": val, 
    "message": "Set %s in /etc/sysctl.conf" % line.strip()
  }
requirements['osparams'] = osparams