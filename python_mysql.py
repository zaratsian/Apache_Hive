
###############################################################################
#
#   SQLAlchemy Connect to MySQL
#
###############################################################################

from sqlalchemy import *

engine = create_engine("mysql+pymysql://root:beasock87@localhost/hockey")
conn = engine.connect()

coach_data = conn.execute('select coachID, g, w from coaches').fetchall()
awards_data = conn.execute('select * from awardsplayers').fetchall()
master_data = conn.execute('select * from master').fetchall()


for record in coach_data[0:10]:
    print record


results = {}

for record in coach_data:
    if record[0] not in results:
        results[record[0]] = [ record[1], record[2] ]
    else:
        results[record[0]] = [ results[record[0]][0]+record[1], results[record[0]][1]+record[2]]

aggregation = []
for key, value in results.items():
    aggregation.append([key, (value[1] / float(value[0])) * 100])

aggregation.sort(key=lambda x: x[1], reverse=True)

for i in aggregation[0:10]:
    print i


#ZEND


