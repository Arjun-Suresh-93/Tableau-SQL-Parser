import sqlparse
import psycopg2
import getpass


class dependencyExtracter:

    def __init__(self, query, host, port, user, dbName):
        self.query = query
        self.host = host
        self.port = port
        self.user = user
        self.dbName = dbName
        self.dbTables = []
        self.selectStatementList = []
        self.tableColumnsDict = {}
        self.tableInstancesDict = {}
        self.tableAliaseDict = {}
        self.tableMetadata={}


    def __del__(self):
        del self.dbTables
        del self.selectStatementList
        del self.tableColumnsDict
        del self.tableInstancesDict
        del self.tableAliaseDict
        del self.tableMetadata


    def findColumnsRecursively(self, current_columnsList, token, metadata_columnslist, selectstmtInstance):
        if token.ttype == sqlparse.tokens.Wildcard:
            for columnName in metadata_columnslist:
                if columnName not in current_columnsList:
                    current_columnsList.append(columnName)
            return -2
        if token.ttype == sqlparse.tokens.DML and token.value.lower() == 'select' and token != selectstmtInstance:
            #Don't proceed to nested select statements. These're handled later
            return -1
        if type(token) == sqlparse.sql.Token:
            if token.value in metadata_columnslist and token.value not in current_columnsList:
                current_columnsList.append(token.value)
        if type(token) != sqlparse.sql.Token:
            for t in token:
                val = self.findColumnsRecursively(current_columnsList, t, metadata_columnslist, selectstmtInstance)
                if val == -2:
                    return val
                if val == -1:
                    return 0
        return 0


    def populateTableNamesFromRedshift(self):
        print("These are the Redshift account details parsed from the tableau:\n"
              "Host: {0}\nPort: {1}\nDatabase name: {2}\n"
              "User name: {3}".format(self.host, self.port, self. dbName, self.user))
        self.password = getpass.getpass("Please enter the password for this account\n")

        try:
            con = psycopg2.connect(dbname=self.dbName,
                                   host=self.host,
                                   port=self.port,
                                   user=self.user,
                                   password=self.password)
        except:
            print("Couldn't connect to the database\n")
            return False

        cur = con.cursor()
        try:
            cur.execute("""SELECT DISTINCT tablename
            FROM pg_table_def
            WHERE schemaname = 'public'
            ORDER BY tablename;""")
            tables = cur.fetchall()
            cur.close()
            con.close()
        except:
            cur.close()
            con.close()
            print("Error while running a query\n")
            return False
        for t in tables:
            self.dbTables.append(t[0])
        del tables
        return True


    def stripSpacesAndGenerateTokensList(self, queryHead):
        tokenQueueFull = []
        tokensList = []
        queryTokens = queryHead[0].tokens
        for token in queryTokens:
            tokenQueueFull.append(token)
        while len(tokenQueueFull) > 0:
            token = tokenQueueFull.pop(0)
            if token.is_whitespace == True:
                parent_token = token.parent
                parent_token.tokens.remove(token)
            else:
                tokensList.append(token)
            if type(token) != sqlparse.sql.Token:
                for t in token:
                    tokenQueueFull.append(t)
        return tokensList


    def extractTableNamesAndAliases(self, validTokensList):
        for t in validTokensList:
            if t.ttype == sqlparse.tokens.DML and t.value.lower() == 'select':
                self.selectStatementList.append(t)
            else:
                if t.ttype == sqlparse.tokens.Name and t.value in self.dbTables:
                    if t.value not in self.tableColumnsDict:
                        col_list = []
                        self.tableColumnsDict[t.value] = col_list
                    if t.value not in self.tableInstancesDict:
                        object_list = [t]
                        self.tableInstancesDict[t.value] = object_list
                    else:
                        object_list = self.tableInstancesDict[t.value]
                        object_list.append(t)
                    parent = t.parent
                    token_index = parent.token_index(t)
                    if token_index < len(parent.tokens) - 1:
                        temp_token = parent.tokens[token_index + 1]
                        count = 1
                        while temp_token.ttype == sqlparse.tokens.Keyword:
                            temp_token = parent.tokens[token_index + count]
                            count += 1
                        if count == 1 or type(temp_token) != sqlparse.sql.Identifier:
                            continue
                        alias_name = temp_token
                        while alias_name.ttype != sqlparse.tokens.Name:
                            alias_name = alias_name.tokens[0]
                        if alias_name.value not in self.tableAliaseDict:
                            self.tableAliaseDict[alias_name.value] = t.value
        for t in validTokensList:
            if t.ttype == sqlparse.tokens.Name and t.value in self.tableAliaseDict:
                alias_instance_list = self.tableInstancesDict[self.tableAliaseDict[t.value]]
                alias_instance_list.append(t)


    def populateDBMetadata(self):
        try:
            con = psycopg2.connect(dbname=self.dbName,
                                   host=self.host,
                                   port=self.port,
                                   user=self.user,
                                   password=self.password)
        except:
            print("Couldn't connect to the database\n")
            return False
        query = """SELECT *
        FROM pg_table_def
        WHERE tablename = '{0}'
        AND schemaname = 'public';"""
        cur = con.cursor()
        for table in self.tableInstancesDict:
            try:
                cur.execute(query.format(table))
                columns = cur.fetchall()
            except:
                cur.close()
                con.close()
                print("Unable to execute query on the database\n")
                return False
            columnList = []
            for column in columns:
                columnList.append(column[2])
            self.tableMetadata[table] = columnList
        cur.close()
        con.close()
        return True


    def extractColumnNamesUsedWithMembership(self):
        for table in self.tableInstancesDict:
            for instance in self.tableInstancesDict[table]:
                parent = instance.parent
                identifierIndex = parent.token_index(instance)
                if identifierIndex < len(parent.tokens) - 1:
                    temp_token = parent.tokens[identifierIndex + 1]
                    if temp_token.ttype == sqlparse.tokens.Punctuation and temp_token.value == '.':
                        columnName_token = parent.tokens[identifierIndex + 2]
                        existing_columnNames = self.tableColumnsDict[table]
                        if columnName_token.value in existing_columnNames:
                            continue
                        else:
                            existing_columnNames.append(columnName_token.value)


    def extractColumnNamesUsedOtherwise(self):
        for selectStmtInstance in self.selectStatementList:
            parent = selectStmtInstance.parent
            for token in parent:
                if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'from':
                    break
            token = parent.tokens[parent.token_index(token) + 1]
            if type(token) != sqlparse.sql.Identifier:
                continue
            tableName = token[0].value
            current_columnsList = self.tableColumnsDict[tableName]
            metadata_columnsList = self.tableMetadata[tableName]
            self.findColumnsRecursively(current_columnsList, parent, metadata_columnsList, selectStmtInstance)


    def extractDependencies(self):
        if self.populateTableNamesFromRedshift() != True:
            return False
        tokensList = self.stripSpacesAndGenerateTokensList()
        self.extractTableNamesAndAliases(tokensList)
        if self.populateDBMetadata()!= True:
            return False
        self.extractColumnNamesUsedWithMembership()
        self.extractColumnNamesUsedOtherwise()