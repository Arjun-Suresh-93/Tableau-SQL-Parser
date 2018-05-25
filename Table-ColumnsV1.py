
# coding: utf-8

# In[29]:


import sqlparse
query = """SELECT t1.id,t2.id FROM t1,t2 WHERE t1.size in (SELECT t3.size from t3)"""


# In[30]:


parsed = sqlparse.parse(query)


# In[31]:


dbTables=[]
import psycopg2

con = psycopg2.connect(dbname='rs_prod',
        host='redshift-prod.yourmechanic.com',
        port='5439',
        user='web_server',
        password='inf0Car1')
cur = con.cursor()
cur.execute("""SELECT DISTINCT tablename
FROM pg_table_def
WHERE schemaname = 'public'
ORDER BY tablename;""")
tables = cur.fetchall()
cur.close()
con.close()
for t in tables:
    dbTables.append(t[0])
del tables
dbTables=['t1','t2','t3']


# In[32]:


queryTokens = parsed[0].tokens
tokenQueueOrig=[]
tokenQueueStripped=[]
for token in queryTokens:
    tokenQueueOrig.append(token)
while len(tokenQueueOrig) > 0:
    token = tokenQueueOrig.pop(0)
    if token.is_whitespace != True:
        tokenQueueStripped.append(token)
    if type(token) != sqlparse.sql.Token:
        for t in token:
            tokenQueueOrig.append(t)


# In[33]:


curTableColumns={}
tableObjects={}
for t in tokenQueueStripped:
    if t.ttype == sqlparse.tokens.Name and t.value in dbTables:
        if t.value not in curTableColumns:
            colList=[]
            curTableColumns[t.value]=colList
        if t.value not in tableObjects:
            objectList = [t]
            tableObjects[t.value]=objectList
        else:
            objectList = tableObjects[t.value]
            objectList.append(t)


# In[34]:


tableObjects


# In[37]:


for table in tableObjects:
    for instance in tableObjects[table]:
        par = instance.parent
        idenIndex = par.token_index(instance)
        if idenIndex < len(par.tokens) - 1:
            testObj = par.tokens[idenIndex+1]
            if testObj.ttype == sqlparse.tokens.Punctuation and testObj.value == '.':
                columnField = par.tokens[idenIndex+2]
                columnNames = curTableColumns[table]
                if columnField.value in columnNames:
                    continue
                else:
                    columnNames.append(columnField.value)


# In[38]:


curTableColumns

