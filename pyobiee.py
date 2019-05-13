import re
from zeep import Client
from requests import Session
from zeep.transports import Transport
import xml.etree.ElementTree as ET
from zeep.wsse.username import UsernameToken
import pandas as pd



# Xml reponse parsing function
def responseXML(wsdl, username, password, reportpath, executionoptions):
    
    # Initializing SOAP client, start a session, and make a connection to XmlViewService binding
    session = Session()
    session.verify = False
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport)
    sessionid = client.service.logon(username, password)
    xmlservice = client.bind('XmlViewService')
    
    
    # Retrieveing data schema and column headings
    schema = xmlservice.executeXMLQuery(report=reportpath, outputFormat="SAWRowsetSchema",
                                       executionOptions=executionoptions, sessionID=sessionid)
    
    columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
    dataset_dict = {}
    
    for head in columnHeading:
        dataset_dict[head] = []
    
    
    # Making a query and parsing first datarows
    queryresult = xmlservice.executeXMLQuery(report=reportpath, outputFormat="SAWRowsetData",
                                            executionOptions=executionoptions, sessionID=sessionid)
    queryid = queryresult.queryID
    ETobject = ET.fromstring(queryresult.rowset)
    namespacerows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
    
    for row in namespacerows:
        for key in dataset_dict.keys():
            dataset_dict[key].append(row.find("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + 
                                             str(list(dataset_dict.keys()).index(key))).text)
    
    
    # Determine if additional fetching is needed and if yes - parsing additional rows   
    queryfetch = queryresult.finished
    
    while (not queryfetch):
        queryfetch = xmlservice.fetchNext(queryID=queryid, sessionID=sessionid)
        ETobject = ET.fromstring(queryfetch.rowset)
        namespacerows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')

        for row in namespacerows:
            for key in dataset_dict.keys():
                dataset_dict[key].append(row.find('{urn:schemas-microsoft-com:xml-analysis:rowset}Column' +
                                                 str(list(dataset_dict.keys()).index(key))).text)
        
        queryfetch = queryfetch.finished
        
    # By some reason OBIEE doesn't make the last fetching, it will fix it
    queryfetch = False
    
    while (not queryfetch):
        queryfetch = xmlservice.fetchNext(queryID=queryid, sessionID=sessionid)
        ETobject = ET.fromstring(queryfetch.rowset)
        namespacerows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
    
        for row in namespacerows:
            for key in dataset_dict.keys():
                dataset_dict[key].append(row.find('{urn:schemas-microsoft-com:xml-analysis:rowset}Column' + 
                                                  str(list(dataset_dict.keys()).index(key))).text)
        queryfetch = True
        
    return pd.DataFrame(dataset_dict)



# Xml reponse parsing function
def responseSQL(wsdl, username, password, query, executionoptions):
    
    # Initializing SOAP client, start a session, and make a connection to XmlViewService binding
    session = Session()
    session.verify = False
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport)
    sessionid = client.service.logon(username, password)
    xmlservice = client.bind('XmlViewService')
    
    
    # Retrieveing data schema and column headings
    schema = xmlservice.executeSQLQuery(sql=query, outputFormat="SAWRowsetSchema",
                                       executionOptions=executionoptions, sessionID=sessionid)
    
    columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
    dataset_dict = {}
    
    for head in columnHeading:
        dataset_dict[head] = []
    
    
    # Making a query and parsing first datarows
    queryresult = xmlservice.executeSQLQuery(sql=query, outputFormat="SAWRowsetData",
                                            executionOptions=executionoptions, sessionID=sessionid)
    queryid = queryresult.queryID
    ETobject = ET.fromstring(queryresult.rowset)
    namespacerows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
    
    for row in namespacerows:
        for key in dataset_dict.keys():
            rowdata = row.find("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + str(list(dataset_dict.keys()).index(key)))
            if rowdata != None:
                dataset_dict[key].append(rowdata.text)
            else:
                dataset_dict[key].append("")
    
    
    # Determine if additional fetching is needed and if yes - parsing additional rows   
    queryfetch = queryresult.finished
    
    while (not queryfetch):
        queryfetch = xmlservice.fetchNext(queryID=queryid, sessionID=sessionid)
        ETobject = ET.fromstring(queryfetch.rowset)
        namespacerows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')

        for row in namespacerows:
            for key in dataset_dict.keys():
                rowdata = row.find("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + str(list(dataset_dict.keys()).index(key)))
                if rowdata != None:
                    dataset_dict[key].append(rowdata.text)
                else:
                    dataset_dict[key].append("")
        
        queryfetch = queryfetch.finished
        
    return pd.DataFrame(dataset_dict)