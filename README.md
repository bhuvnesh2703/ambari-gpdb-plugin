Ambari HAWQ plugin
=================================

How to install
--------------------
This section describes how to install hawq plugin onto ambari-server.

If your Ambari cluster is a single node cluster, there are some extra steps you'd have to follow

1. Stop ambari-server
1. Make sure you execute setup_repo.sh for both PADS and hawq-plugin

    ```
    # cd to where PADS.tar and hawq-plugin.tar files are extracted
    ./setup_repo.sh
    ```

1. Install the plugin:
    ```
    sudo yum install -y hawq-plugin
    ```
1. **Single node cluster only** Before starting ambari-server, remove HAWQSTANDBY component from metainfo.xml. See Appendix 1
1. **Single node cluster only** Modify output.replace-datanode-on-failure parameter in /var/lib/ambari-server/resources/stacks/PHD/3.0/services/HAWQ/package/templates/hdfs-client.j2 to false
1. Start ambari-server
1. **Single node cluster only** During Cluster Install Wizard, make sure hawq.master.port in hawq-site is changed through Ambari Wizard to 10432 (or anything other than 5432) to avoid a port conflict.

#### Appendix 1 What <component> to remove from metainfo.xml
e.g /var/lib/ambari-server/resources/stacks/PHD/3.0/services/HAWQ/metainfo.xml

```
<component>
    <name>HAWQSTANDBY</name>
    <displayName>HAWQ Standby Master</displayName>
    <category>MASTER</category>
    <cardinality>1</cardinality>
    <commandScript>
        <script>scripts/hawqstandby.py</script>
        <scriptType>PYTHON</scriptType>
        <timeout>600</timeout>
    </commandScript>
    <customCommands>
        <customCommand>
            <name>ActivateStandby</name>
            <commandScript>
                <script>scripts/hawqstandby.py</script>
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


