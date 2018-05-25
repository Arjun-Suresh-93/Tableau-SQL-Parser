import sqlparse
import sys
import psycopg2


dependencies={}

DBTables=[]

def populate_tables():
    global DBTables
    #hardcoding the host,password and d details as of now. Need to parse it from tableau when integrating later
    con = psycopg2.connect(dbname='rs_prod',
		       host='redshift-prod.yourmechanic.com',
		       port='5439',
		       user='web_server',
		       password='inf0Car1')
    curs = con.cursor()
    curs.execute("""SELECT DISTINCT tablename
FROM pg_table_def
WHERE schemaname = 'public'
ORDER BY tablename;""")
    tables = curs.fetchall()
    for table in tables:
        DBTables.append(table[0])


def scanTree(queryTree):
    global dependencies
    tokenQueue = []
    queryTokens = queryTree.tokens
    for token in queryTokens:
        tokenQueue.append(token)
    while len(tokenQueue)>0:
        token = tokenQueue.pop(0)
        if type(token) == sqlparse.sql.Token and token.ttype == sqlparse.tokens.Name:
            par = token.parent
            while type(par) != sqlparse.sql.Identifier:
                par = par.parent



def process_query(query):
    queryTrees = sqlparse.parse(query)
    populate_tables()
    for queryTree in queryTrees:
        temporaries=[]
        scanTree(queryTree)



query = sys.argv(2)
process_query(query)
for table in dependencies:
    print (table+" : ")
    for field in dependencies[table]:
        print (field)

