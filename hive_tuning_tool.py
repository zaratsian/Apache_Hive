




def hive_tuning(total_ram_gb=(9.87 * 1), cores_per_node=4, nodes=1):
    
    recommendations = []
    print '\n'
    
    ###########################################################################################
    #
    #   Top Recommendations
    #
    ###########################################################################################
    
    print '\n\n'
    print '####################################################################################'
    print '(1) Use Tez:'
    print 'Tez avoids unneeded writes to HDFS'
    param = 'set hive.execution.engine=tez;'
    print param
    recommendations.append(param)
    param = 'set hive.tez.container.size=4096'
    print param
    recommendations.append(param)
    
    print '\n\n'
    print '####################################################################################'
    print '(2) Use Vectorization:'
    print 'Vectorized query execution streamlines operations by processing a block of'
    print '1024 rows at a time (instead of 1 row at a time), reducing the CPU usage.'
    param = 'set hive.vectorized.execution.enabled = true;'
    print param
    recommendations.append(param)
    param = 'set hive.vectorized.execution.reduce.enabled = true;'
    print param
    recommendations.append(param)
    
    print '\n\n'
    print '####################################################################################'
    print '(3) Use CBO (Cost-based Optimization):'
    print 'The CBO engine uses statistics within Hive tables to produce optimal query plans.'
    print 'Table stats and column stats can be used for optimization. Calcite used for this.'
    param = 'set hive.cbo.enable=true;'
    print param
    recommendations.append(param)
    param = 'set hive.compute.query.using.stats=true;'
    print param
    recommendations.append(param)
    param = 'set hive.stats.fetch.column.stats=true;'
    print param
    recommendations.append(param)
    param = 'set hive.stats.fetch.partition.stats=true;'
    print param
    recommendations.append(param)
    param = 'set hive.stats.autogather=true;'
    print param
    recommendations.append(param)
    print '\nCBO Syntax:'
    print 'analyze table myTable compute statistics;'
    print 'analyze table myTable compute statistics for columns;'
    print 'analyze table myTable compute statistics for columns nameCol, stateCol;'
    print "analyze table myTable partition (col1='x') compute statistics;"
    
    print '\n\n'
    print '####################################################################################'
    print '(4) Use ORC with Zlib or Snappy compression'
    print 'SNAPPY is a good tradeoff between compression and performance.'
    print 'ORCFile sor1ng helps with vectorization.'
    print '\nORC Syntax:'
    print 'CREATE TABLE A_ORC (customerID int, name string, age int, address string) STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");'
    print 'INSERT INTO TABLE A_ORC SELECT * FROM A;'
    
    ###########################################################################################
    #
    #   General Recommendations
    #
    ###########################################################################################
    
    print '\n\n'
    print '####################################################################################'
    print 'YARN Configuration'
    yarn_nodemanager_resource_memory_mb = (total_ram_gb * 1000) * .80
    param = 'set yarn.nodemanager.resource.memory.mb = ' + str(yarn_nodemanager_resource_memory_mb) + ';'
    print param
    recommendations.append(param)
    
    print '\n\n'
    print '####################################################################################'
    print 'Pre-warm Configuration'
    prewarm_containers  = 25
    session_per_default = (total_ram_gb * .85) / (2 * prewarm_containers)
    param = 'set hive.prewarm.numcontainers = ' + str(prewarm_containers)
    print param
    recommendations.append(param)
    param = 'set hive.server2.tez.session.per.default.queue = ' + str(session_per_default)
    print param
    recommendations.append(param)
    param = 'set tez.am.container.idle.release-timeout-max-millis=3600000'
    print param
    recommendations.append(param)
    
    ######################################################################################
    #
    #   Calculated Recommendations
    #
    ######################################################################################
    
    print '\n\n'
    print '####################################################################################'
    print 'Partitioning & Bucketing'
    print '####################################################################################'
    print 'Loading Data into Partitioned Tables'
    print 'Loading Data with Dynamic Partitions'
    print 'You May Need to Re-Order Columns'
    print 'Bucketing: defining bucketed tables'
    print 'Bucketing: writing data into bucketed tables'
    print 'Skewed Tables and List Bucketing'
    print 'When to use Partitioning/Bucketing'
    
    print '\n\n'
    print 'All Recommendations:'
    print '\n'
    for i in recommendations:
        print str(i)
    
    print '\n\n'
    return recommendations



hive_tuning(total_ram_gb=(9.87 * 1), cores_per_node=4, nodes=1)


'''
param = 'xxxxx'
print param
recommendations.append(param)
'''

#ZEND