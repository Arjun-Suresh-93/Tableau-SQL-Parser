
# coding: utf-8

# In[122]:


import sqlparse
query = """SELECT
pa.id,
pa.token,
pa.type,
CONVERT_TIMEZONE('PST8PDT', pa.ts) As p_ts,
pa.payload,
act.event_type,
act.ts,
visitors.client,
visitors.visitor_type
FROM posted_activities As pa
JOIN visitors ON (pa.token = visitors.token)
LEFT JOIN (SELECT * FROM activities WHERE event_type = 'appointment:affirm_checkout_complete' OR event_type = 'appointment:affirm_cancel') As act ON (pa.token = act.token AND DATE_TRUNC('day', CONVERT_TIMEZONE('PST8PDT', pa.ts)) = DATE_TRUNC('day', CONVERT_TIMEZONE('PST8PDT', act.ts)) AND pa.type = 'Checkout requested')
WHERE
    pa.type = 'Quote Requested'
    OR (pa.type = 'Selected payment method' AND pa.payload LIKE '%affirm%')
    OR (pa.type = 'Checkout requested' AND pa.payload LIKE '%affirm%')"""


# In[123]:


parsed = sqlparse.parse(query)


# In[124]:


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


# In[125]:


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


# In[126]:


curTableColumns={}
tableObjects={}
tableAliases={}
selectstmts=[]
for t in tokenQueueStripped:
    if t.ttype == sqlparse.tokens.DML and t.value.lower() == 'select':
        selectstmts.append(t)
    else:
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
            par = t.parent
            idenIndex = par.token_index(t)
            if idenIndex < len(par.tokens) - 1:
                testObj = par.tokens[idenIndex+1]
                k=1
                while testObj.ttype == sqlparse.tokens.Whitespace or testObj.ttype == sqlparse.tokens.Newline or testObj.ttype == sqlparse.tokens.Keyword:
                    testObj = par.tokens[idenIndex+k]
                    k+=1
                if k == 1 or type(testObj) != sqlparse.sql.Identifier:
                    continue
                aliasName = testObj
                while aliasName.ttype != sqlparse.tokens.Name:
                    aliasName = aliasName.tokens[0]
                if aliasName.value not in tableAliases:
                    tableAliases[aliasName.value] = t.value


# In[127]:


selectstmts


# In[128]:


for t in tokenQueueStripped:
    if t.ttype == sqlparse.tokens.Name and t.value in tableAliases:
        instanceList = tableObjects[tableAliases[t.value]]
        instanceList.append(t)


# In[129]:


tableMetadata={}
con = psycopg2.connect(dbname='rs_prod',
        host='redshift-prod.yourmechanic.com',
        port='5439',
        user='web_server',
        password='inf0Car1')
query = """SELECT *
FROM pg_table_def
WHERE tablename = '{0}'
AND schemaname = 'public';"""
cur=con.cursor()
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
    cur.execute(query.format(table))
    columns = cur.fetchall()
    columnList=[]
    for column in columns:
        columnList.append(column[2])
    tableMetadata[table]=columnList
cur.close()
con.close()
    


# In[130]:


curTableColumns


# In[133]:


def findColumnsRecursively(columnsList, token, metaDataColumnsList, selectstmt):
    if token.ttype == sqlparse.tokens.Wildcard:
        for field in metaDataColumnsList:
            if field not in columnsList:
                columnsList.append(field)
        return -2
    if token.ttype == sqlparse.tokens.DML and token.value.lower() == 'select' and token != selectstmt:
        return -1
    if type(token) == sqlparse.sql.Token:
        if token.value in metaDataColumnsList and token.value not in columnsList:
            columnsList.append(token.value)
    if type(token) != sqlparse.sql.Token:
        for t in token:
            val = findColumnsRecursively(columnsList, t, metaDataColumnsList, selectstmt)
            if val == -2:
                return val
            if val == -1:
                return 0
    return 0


for stmt in selectstmts:
    subquery = stmt.parent
    for token in subquery:
        if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'from':
            break
    k=subquery.token_index(token)+1
    token=subquery.tokens[k]
    while token.ttype == sqlparse.tokens.Whitespace:
        k+=1
        token=subquery.tokens[k]
    if type(token) != sqlparse.sql.Identifier:
        continue
    tableName = token[0].value
    columnsList = curTableColumns[tableName]
    metaDataColumnsList = tableMetadata[tableName]
    findColumnsRecursively(columnsList, subquery, metaDataColumnsList, stmt)


# In[134]:


curTableColumns

