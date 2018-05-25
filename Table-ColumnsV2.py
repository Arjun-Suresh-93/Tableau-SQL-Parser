
# coding: utf-8

# In[66]:


import sqlparse
query = """SELECT click.mechanic_id,
    convert_timezone('PST8PDT', click.ts) as click_ts,
    click.clicked_type,
    convert_timezone('PST8PDT',ac.ts) as add_ts,
    ac.added_type,
    ac.value,
    m.name
FROM
    (SELECT mechanic_id, ts, 
    regexp_replace( regexp_substr(type, 'clicked_[^_]*'), 'clicked_') as clicked_type
    FROM posted_activities
    WHERE type in ('mechanic:clicked_timeslot_make_more_money',
                'mechanic:clicked_zipcode_make_more_money',
                'mechanic:clicked_tool_make_more_money')
    ) click
JOIN dw_mechanics m
ON m.id = click.mechanic_id
LEFT JOIN
    (SELECT mechanic_id, ts, value,
        regexp_replace( regexp_substr(text, 'Mechanic added a [\\S]*'), 'Mechanic added a ') as added_type
    FROM activities
    WHERE event_type = 'mechanic:add_capability'
    ) ac
ON click.mechanic_id = ac.mechanic_id
    AND click.clicked_type = ac.added_type
    AND click.ts <<= ac.ts"""


# In[67]:


parsed = sqlparse.parse(query)


# In[68]:


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


# In[69]:


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


# In[72]:


curTableColumns={}
tableObjects={}
tableAliases={}
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
        par = t.parent
        idenIndex = par.token_index(t)
        if idenIndex < len(par.tokens) - 1:
            testObj = par.tokens[idenIndex+1]
            k=1
            while testObj.ttype == sqlparse.tokens.Whitespace or testObj.ttype == sqlparse.tokens.Keyword:
                testObj = par.tokens[idenIndex+k]
                k+=1
            if k == 1 or type(testObj) != sqlparse.sql.Identifier:
                continue
            aliasName = testObj
            while aliasName.ttype != sqlparse.tokens.Name:
                aliasName = aliasName.tokens[0]
            if aliasName.value not in tableAliases:
                tableAliases[aliasName.value] = t.value


# In[73]:


tableAliases


# In[74]:


for t in tokenQueueStripped:
    if t.ttype == sqlparse.tokens.Name and t.value in tableAliases:
        instanceList = tableObjects[tableAliases[t.value]]
        instanceList.append(t)


# In[75]:


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


# In[76]:


curTableColumns

