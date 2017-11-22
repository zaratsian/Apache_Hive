
'''

pip install pyhive
pip install thrift
pip install sasl
pip install thrift_sasl

'''

from pyhive import hive

outputfile = open('/Users/dzaratsian/Desktop/hive_metadata.txt','wb')

c = hive.connect('dzaratsian1.field.hortonworks.com').cursor()
c.execute('show databases')
databases = c.fetchall()

tables = []
for database in databases:
    c.execute('show tables')
    table = c.fetchall()
    tables = tables + table

output = {}

for table in tables:
    
    c.execute('describe formatted ' + str(table[0]))
    table_details = None
    table_details = c.fetchall()
    
    Database    = [i[1].strip() for i in table_details if i[0].strip() == 'Database:'][0]
    Owner       = [i[1].strip() for i in table_details if i[0].strip() == 'Owner:'][0]
    Location    = [i[1].strip() for i in table_details if i[0].strip() == 'Location:'][0]
    
    numFiles    = [i[2].strip() for i in table_details if i[1] != None and i[1].strip() == 'numFiles'][0]
    numRows     = [i[2].strip() for i in table_details if i[1] != None and i[1].strip() == 'numRows'][0]
    rawDataSize = [i[2].strip() for i in table_details if i[1] != None and i[1].strip() == 'rawDataSize'][0]
    
    totalSize   = [i[2].strip() for i in table_details if i[1] != None and i[1].strip() == 'totalSize'][0]
    
    output[str(Database) + '.' + str(table[0])] = {
                                                    'database':Database, 
                                                    'table':table[0], 
                                                    'owner':Owner, 
                                                    'location':Location, 
                                                    'numFiles':numFiles, 
                                                    'numRows':numRows,
                                                    'rawDataSize':rawDataSize,
                                                    'totalSize':totalSize
                                                  }
    
    print '\n\n[ INFO ] Collected metadata for ' + str(Database) + '.' + str(table[0])
    print 'Database:     ' + str(Database)
    print 'Owner:        ' + str(Owner)
    print 'Location:     ' + str(Location)
    print 'numFiles:     ' + str(numFiles)
    print 'numRows:      ' + str(numRows)
    print 'rawDataSize:  ' + str(rawDataSize)
    print 'totalSize:    ' + str(totalSize)
    print 'Database:     ' + str(Database)
    
    '''
    outputfile.write('\n\n[ INFO ] Collected metadata for ' + str(Database) + '.' + str(table[0]))
    outputfile.write('\nDatabase:     ' + str(Database))
    outputfile.write('\nOwner:        ' + str(Owner))
    outputfile.write('\nLocation:     ' + str(Location))
    outputfile.write('\nnumFiles:     ' + str(numFiles))
    outputfile.write('\nnumRows:      ' + str(numRows))
    outputfile.write('\nrawDataSize:  ' + str(rawDataSize))
    outputfile.write('\ntotalSize:    ' + str(totalSize))
    outputfile.write('\nDatabase:     ' + str(Database))
    '''



#output['default.ratings']

'''


def get_hive_metadata(db, table):
    for k,v in output[db+'.'+table].iteritems():
        print (k,v)


'''

#ZEND
