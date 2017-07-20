
################################################################################################
#
#   Python UDF for Apache Hive
#
#   INPUT:  tab delimited string received via stdin
#   OUTPUT: tab delimited string sent via print/stdout
#
################################################################################################

import sys

for line in sys.stdin:
    
    id, text = line.replace('\n',' ').split('\t')
    
    positive = set(["love", "good", "great", "happy", "cool", "best", "awesome", "nice"])
    negative = set(["hate", "bad", "stupid"])
    
    words = text.split()
    word_count = len(words)
    
    positive_matches = [1 for word in words if word in positive]
    negative_matches = [-1 for word in words if word in negative]
    
    st = sum(positive_matches) + sum(negative_matches)
    
    if st > 0:
        print '\t'.join([id, text, 'positive', str(word_count)])
    elif st < 0:
        print '\t'.join([id, text, 'negative', str(word_count)])
    else:
        print '\t'.join([id, text, 'neutral', str(word_count)])


################################################################################################
#
#   Corresponding Hive Script
#
################################################################################################

'''
ADD FILE /home/hive/my_py_udf.py;
SELECT
TRANSFORM (id, text)
USING 'python pyfunc3.py'
AS  id,  
    text, 
    sentiment,
    word_count
FROM tweets;
'''

#ZEND
