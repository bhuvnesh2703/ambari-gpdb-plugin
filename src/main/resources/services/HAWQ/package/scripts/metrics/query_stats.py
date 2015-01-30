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
import psycopg2
from common import Utils, MetricValue

class QueryMemoryStats(object):
    def __init__(self, conn_params):
        self.conn_params = conn_params

    def get(self):
        mem_stats = []
        aggregated_host_mem = 0
        aggregated_host_cpu = 0
        for seg_num in xrange(self.conn_params.get('num_segments')):
            dsn = Utils.Dsn(host=str(self.conn_params.get('connection_host')),
                      port=self.conn_params.get('base_port') + seg_num,
                      dbname=str(self.conn_params.get('dbname')),
                      user=str(self.conn_params.get('username')),
                      password=str(self.conn_params.get('password')))
            db_conn = None
            try:
                db_conn = psycopg2.connect(dsn.build())
                db_curs = db_conn.cursor()

                query = "select datname, procpid, sess_id, usename, current_query, waiting, query_start from pg_stat_activity where sess_id <> -1 order by query_start;"
                db_curs.execute(query)
                results = db_curs.fetchall()
            finally:
                if db_conn:
                    db_conn.close()
            if results:
                for (datname, procpid, sess_id, usename, current_query, waiting, query_start) in results:
                    (mem, cpu) = Utils.get_proc_stats(procpid)
                    aggregated_host_mem += mem
                    aggregated_host_cpu += cpu
                    # mem_metric_name = "Query-Memory-{0}".format(sess_id)
                    # mem_stats.append(MetricValue(mem_metric_name,  mem, "int32", False))


        mem_stats.append(MetricValue("hawq.QueryMetrics.AggregatedHostMemory", aggregated_host_mem,
                                     "int32", True))
        mem_stats.append(MetricValue("hawq.QueryMetrics.AggregatedHostCPU", aggregated_host_cpu,
                                     "int32", True))

        return mem_stats  