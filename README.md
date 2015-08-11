Ambari Greenplum database plugin
=================================

How to install
--------------------
This section describes how to install greenplum plugin onto ambari-server.

If your Ambari cluster is a single node cluster, there are some extra steps you'd have to follow

1. Stop ambari-server
1. Make sure you execute setup_repo.sh for both GPDB and gpdb-plugin

    ```
    # cd to where GPDB.tar and greenplum-plugin.tar files are extracted
    ./setup_repo.sh
    ```

1. Install the plugin:
    ```
    sudo yum install -y greenplum-plugin
    ```
1. **Single node cluster only** Before starting ambari-server, remove GPDBSTANDBY component from metainfo.xml. See Appendix 1
1. Start ambari-server
1. **Single node cluster only** During Cluster Install Wizard, make sure greenplum.master.port in greenplum-site is changed through Ambari Wizard to 10432 (or anything other than 5432) to avoid a port conflict.

#### Appendix 1 What <component> to remove from metainfo.xml
e.g /var/lib/ambari-server/resources/stacks/PHD/3.0/services/GPDB/metainfo.xml

```
<component>
    <name>GPDBSTANDBY</name>
    <displayName>Greenplum Standby Master</displayName>
    <category>MASTER</category>
    <cardinality>1</cardinality>
    <commandScript>
        <script>scripts/greenplumstandby.py</script>
        <scriptType>PYTHON</scriptType>
        <timeout>600</timeout>
    </commandScript>
    <customCommands>
        <customCommand>
            <name>ActivateStandby</name>
            <commandScript>
                <script>scripts/greenplumstandby.py</script>
                <scriptType>PYTHON</scriptType>
                <timeout>600</timeout>
            </commandScript>
        </customCommand>
    </customCommands>
    <dependencies>
      <dependency>
          <name>HDFS/HDFS_CLIENT</name>
          <scope>host</scope>
          <auto-deploy>
              <enabled>true</enabled>
          </auto-deploy>
      </dependency>
    </dependencies>
</component>

```


