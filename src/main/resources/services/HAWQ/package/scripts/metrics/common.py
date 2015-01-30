"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
class Utils(object):
    @staticmethod
    def get_proc_stats(pid):
        import subprocess
        out = subprocess.Popen(['ps', 'v', '-p', str(pid)], stdout=subprocess.PIPE).communicate()[0].split('\n')
        rss_index = out[0].split().index(b'RSS')
        cpu_index = out[0].split().index(b'%CPU')
        stats_splits = out[1].split()
        mem_rss = float(stats_splits[rss_index]) / 1024
        cpu_percentage = float(stats_splits[cpu_index])
        return (mem_rss, cpu_percentage)

    @staticmethod
    def publish_gmetric(conf_path, metric_name, metric_value, value_type, group=None):
        import subprocess
        if group:
            command = "gmetric -c {0} -n {1} -v {2} -t {3} -g {4}".format(conf_path, metric_name, metric_value, value_type, group)
        else:
            command = "gmetric -c {0} -n {1} -v {2} -t {3}".format(conf_path, metric_name, metric_value, value_type)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, error) = process.communicate()
        retcode = process.wait()
        return (retcode, out, error)

    class Dsn(object):
        def __init__(self, host, port, dbname, user, password):
            self.dbname = dbname
            self.host = host
            self.port = port
            self.user = user
            self.password = password

        def build(self):
            return "dbname=" + self.dbname + " host=" + self.host + " user=" + self.user  + " port=" + str(self.port) + " options='-c gp_maintenance_conn=true -c gp_session_role=utility'"

class MetricValue(object):
    def __init__(self, name, value=None, type_str="int32", enabled=True):
        self.name =  name
        self.value = value
        self.enabled = enabled
        self.type_str = type_str