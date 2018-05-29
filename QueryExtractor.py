import xml.etree.ElementTree as ET

class queryPkg:
    def __init__(self, query, host, port, user, dbName):
        self.query = query
        self.host = host
        self.port = port
        self.user = user
        self.dbName = dbName


class sqlQueryExtractor:
    def __init__(self, filename):
        self.tableau_file = filename
        self.query_objects = []
        self.has_custom_sql_query = False


    def __del__(self):
        for queryObj in self.query_objects:
            del queryObj
        del self.query_objects


    def populateQueries(self):
        try:
            tree = ET.parse(self.tableau_file)
        except ET.ParseError:
            return
        root = tree.getroot()
        elements = []
        elements.append(root)
        parent_element = None
        while len(elements) > 0:
            element = elements.pop(0)
            if element.tag == 'relation' and 'type' in element.attrib and element.attrib['type'] == 'text' and 'SELECT' in element.text:
                host = parent_element.attrib['server']
                port = parent_element.attrib['port']
                user = parent_element.attrib['username']
                dbName = parent_element.attrib['dbname']
                self.query_objects.append(queryPkg(element.text, host, port, user, dbName))
                self.has_custom_sql_query = True
            else:
                for item in element:
                    elements.append(item)
                parent_element = element
