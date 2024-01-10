import psycopg2
from itertools import islice
from io import StringIO


def loadRatings(ratingstablename,ratingsfilepath,openconnection):
    with open(ratingsfilepath) as f:
        lines_per_batch=5000
        batch=StringIO()
        
        cursor=openconnection.cursor()
        cursor.execute("DROP TABLE IF EXISTS {}".format(ratingstablename))
        cursor.execute(f"CREATE TABLE {ratingstablename}(userid int not null,movieid int ,rating real,timestamp int);")
        
        for line in f:
            line=line.replace('::',',')
            batch.write(line)
            
            if batch.tell()>=lines_per_batch:
                batch.seek(0)
                cursor.copy_from(batch,ratingstablename,sep=',',columns=('userid','movieid','rating','timestamp'))
        if batch.tell()>0:
            batch.seek(0)
            cursor.copy_from(batch,ratingstablename,sep=',',columns=('userid','movieid','rating','timestamp'))
        cursor.execute(f"ALTER TABLE{ratingstablename} DROP COLUMN timestamp")
        cursor.close()
        
        

def rangePartition(ratingstablename,numberofpartitions,openconnection):
    stepsize=5.0 / numberofpartitions
    createpart_init='CREATE TABLE range_part{0} AS SELECT *FROM {1} WHERE rating>={2} AND rating <={3}'
    createpart='CREATE TABLE range_part{0} AS SELECT * FROM {1} WHERE rating >{2} AND rating<={3}'
    
    with openconnection.cursor() as cursor:
        for i in range(numberofpartitions):
            if i==0:
                cursor.execute(createpart_init.format(i,ratingstablename,i*stepsize,(i+1)*stepsize))
            else:
                cursor.execute(createpart.format(i,ratingstablename,i*stepsize,(i+1)*stepsize))
                
                
    
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    createrr = '''
    CREATE TABLE rrobin_part{0} AS 
    SELECT userid, movieid, rating 
    FROM (
        SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid 
        FROM {1}
    ) AS temp
    WHERE MOD(temp.rowid - 1, {2}) = {3}'''
    
    with openconnection.cursor() as cursor:
        for i in range(numberofpartitions):
            cursor.execute(createrr.format(i, ratingstablename, numberofpartitions, i))

    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cursor:
        cursor.execute("INSERT INTO {0} VALUES (%s, %s, %s)".format(ratingstablename), (userid, itemid, rating))
        cursor.execute("SELECT * FROM {0}".format(ratingstablename))
        numrecords = len(cursor.fetchall())
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%'")
        numparts = len(cursor.fetchall())
        tbid = (numrecords - 1) % numparts
        cursor.execute("INSERT INTO rrobin_part{0} VALUES (%s, %s, %s)".format(tbid), (userid, itemid, rating))
    
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cursor:
        cursor.execute("INSERT INTO {0} VALUES (%s, %s, %s)".format(ratingstablename), (userid, itemid, rating))
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%'")
        numparts = len(cursor.fetchall())
        insertrng = "INSERT INTO range_part{0} VALUES (%s, %s, %s)"
        stepsize = 5.0 / numparts
        for i in range(numparts):
            if i == 0:
                if rating >= i * stepsize and rating <= (i + 1) * stepsize:
                    cursor.execute(insertrng.format(i), (userid, itemid, rating))
            else:
                if rating > i * stepsize and rating <= (i + 1) * stepsize:
                  cursor.execute(insertrng.format(i), (userid, itemid, rating))
