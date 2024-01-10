import psycopg2
import os
import sys
  
    
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    result = []
    cursor = openconnection.cursor()

    # Query to retrieve partition numbers for range ratings
    partquery = '''
        SELECT partitionnum 
        FROM rangeratingsmetadata 
        WHERE maxrating >= %s AND minrating <= %s;
    '''
    cursor.execute(partquery, (ratingMinValue, ratingMaxValue))
    partitions = [partition[0] for partition in cursor.fetchall()]

    # Query to select ratings from each range partition
    rangeselectquery = '''
        SELECT * 
        FROM rangeratingspart{0} 
        WHERE rating >= %s AND rating <= %s;
    '''

    # Retrieve ratings from each range partition and store the results
    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition), (ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = ['RangeRatingsPart{}'.format(partition)] + list(res)
            result.append(res)

    # Query to retrieve the number of partitions for round-robin ratings
    rrcountquery = '''
        SELECT partitionnum 
        FROM roundrobinratingsmetadata;
    '''
    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    # Query to select ratings from each round-robin partition
    rrselectquery = '''
        SELECT * 
        FROM roundrobinratingspart{0} 
        WHERE rating >= %s AND rating <= %s;
    '''

    # Retrieve ratings from each round-robin partition and store the results
    for i in range(rrparts):
        cursor.execute(rrselectquery.format(i), (ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = ['RoundRobinRatingsPart{}'.format(i)] + list(res)
            result.append(res)

    writeToFile('RangeQueryOut.txt', result)

def writeToFile(filename, data):
    with open(filename, 'w') as file:
        for row in data:
            file.write(','.join(str(item) for item in row))
            file.write('\n')
            


    
def PointQuery(ratingsTableName, ratingValue, openconnection):
    result = []
    cursor = openconnection.cursor()
    
    # Retrieve partition numbers for range ratings
    partquery = '''
    SELECT partitionnum 
    FROM rangeratingsmetadata 
    WHERE maxrating >= {0} AND minrating <= {0};
    '''.format(ratingValue)
    cursor.execute(partquery)
    partitions = [partition[0] for partition in cursor.fetchall()]

    # Execute range select query for each partition
    rangeselectquery = '''
    SELECT * 
    FROM rangeratingspart{0} 
    WHERE rating = {1};
    '''
    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = ['RangeRatingsPart{}'.format(partition)] + list(res)
            result.append(res)

    # Retrieve number of partitions for round-robin ratings
    rrcountquery = '''
    SELECT partitionnum 
    FROM roundrobinratingsmetadata;
    '''
    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    # Execute round-robin select query for each partition
    rrselectquery = '''
    SELECT * 
    FROM roundrobinratingspart{0} 
    WHERE rating = {1};
    '''
    for i in range(rrparts):
        cursor.execute(rrselectquery.format(i, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = ['RoundRobinRatingsPart{}'.format(i)] + list(res)
            result.append(res)

    writeToFile('PointQueryOut.txt', result)

def writeToFile(filename, rows):
    with open(filename, 'w') as f:
        for line in rows:
            f.write(','.join(str(s) for s in line))
            f.write('\n')
