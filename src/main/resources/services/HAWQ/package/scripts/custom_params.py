# dfs.allow.truncate in hdfs-site.xml being true is important for hawq's performance but still operational with a degraded performance.
# This flag is to decide whether starting hawq completely fails or still starts with the performance limitation when the truncate property is set to false.
enforce_hdfs_truncate=True

