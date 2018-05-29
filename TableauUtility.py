from QueryExtractor import *
from DependencyExtractor import dependencyExtracter
import sys

def main(filename):
    queryExtractor_obj = sqlQueryExtractor(filename)
    queryExtractor_obj.populateQueries()
    if queryExtractor_obj.has_custom_sql_query:
        for queryObj in queryExtractor_obj.query_objects:
            dependencyExtracter_obj = dependencyExtracter(queryObj.query, queryObj.host, queryObj.port, queryObj.user, queryObj.dbName)
            if dependencyExtracter_obj.extractDependencies() == True:
                print("*"*500)
                tableColumnsDict = dependencyExtracter_obj.tableColumnsDict
                for table in tableColumnsDict:
                    print("Table:\t"+table)
                    columnsList = tableColumnsDict[table]
                    print("Columns:")
                    for column in columnsList:
                        print(column)


if __name__ == '__main__':
    if (len(sys.argv) != 2):
        print ('usage:\tTableauUtility.py <tableau_filename>')
        sys.exit(0)
    main(sys.argv[1])