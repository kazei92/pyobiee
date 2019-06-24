import re
import time
from zeep import Client
from requests import Session
from zeep.transports import Transport
import xml.etree.ElementTree as ET
from zeep.wsse.username import UsernameToken
import pandas as pd


def downloadReport(wsdl, username, password, reportpath, executionOptions):
    
    """
    The function connects to the OBIEE and download report's data, returnes Pandas DataFrame
    """


    # Initializing SOAP client, start a session, and make a connection to XmlViewService binding
    session = Session()
    session.verify = True
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport)
    sessionid = client.service.logon(username, password)
    xmlservice = client.bind('XmlViewService')
    
    
    # Retrieveing data schema and column headings
    max_retries = 30
    while max_retries > 0:
        schema = xmlservice.executeXMLQuery(report=reportpath, outputFormat="SAWRowsetSchema", executionOptions=executionOptions, sessionID=sessionid)
        if schema.rowset == None:
            max_retries -= 1
            continue
        else:
            time.sleep(10)
            break
    
    if schema.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise SAWRowsetSchemaError
    
    columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
    dataset_dict = {}
    
    for head in columnHeading:
        dataset_dict[head] = []
    
    
    # Making a query and parsing first datarows
    queryresult = xmlservice.executeXMLQuery(report=reportpath, outputFormat="SAWRowsetData",
                                                executionOptions=executionOptions, sessionID=sessionid)
    queryid = queryresult.queryID

    if queryresult.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise SAWRowsetDataError
    
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

def executeSQL(wsdl, username, password, query, executionOptions):
    
    """
    The function sends a SQL query to OBIEE for execution and then downloads data, returnes Pandas DataFrame
    """


    # Initializing SOAP client, start a session, and make a connection to XmlViewService binding
    session = Session()
    session.verify = True
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport)
    sessionid = client.service.logon(username, password)
    xmlservice = client.bind('XmlViewService')
    
    
    # Retrieveing data schema (column headers)
    # Max retries per query
    max_retries = 30
    while max_retries > 0:
        schema = xmlservice.executeSQLQuery(sql=query, outputFormat="SAWRowsetSchema", executionOptions=executionOptions, sessionID=sessionid)
        if schema.rowset == None:
            max_retries -= 1
            continue
        else:
            time.sleep(20)
            break
    
    if schema.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise SAWRowsetSchemaError
    
    columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
    dataset_dict = {}
    
    for head in columnHeading:
        dataset_dict[head] = []
    
    
    # Making a query and parsing first datarows
    queryresult = xmlservice.executeSQLQuery(sql=query, outputFormat="SAWRowsetData",
                                            executionOptions=executionOptions, sessionID=sessionid)
    queryid = queryresult.queryID

    if queryresult.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise SAWRowsetDataError

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
    
    client.service.logoff(sessionID=sessionid)
        
    return pd.DataFrame(dataset_dict)

def executeSQLmulti(wsdl, username, password, queries, executionOptions):
    
    # Initializing SOAP client, start a session, and make a connection to XmlViewService binding
    session = Session()
    session.verify = True
    transport = Transport(session=session)
    client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport)
    sessionid = client.service.logon(username, password)
    xmlservice = client.bind('XmlViewService')

    data = {}
    
    for queryname in queries.keys():
        # Number of attemts to retrieve SAWRowsetSchema
        max_retries = 30
        while max_retries > 0:
            schema = xmlservice.executeSQLQuery(sql=queries[queryname], outputFormat="SAWRowsetSchema", executionOptions=executionOptions, sessionID=sessionid)
            if schema.rowset == None:
                max_retries -= 1
                continue
            else:
                break

    
        if schema.rowset == None:
            client.service.logoff(sessionID=sessionid)
            raise SAWRowsetSchemaError
    
        columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
        dataset_dict = {}
    
        for head in columnHeading:
            dataset_dict[head] = []
    
    
        # Making a query and parsing first datarows
        queryresult = xmlservice.executeSQLQuery(sql=queries[queryname], outputFormat="SAWRowsetData", 
                                                    executionOptions=executionOptions, sessionID=sessionid)
        queryid = queryresult.queryID

        if queryresult.rowset == None:
            client.service.logoff(sessionID=sessionid)
            raise SAWRowsetDataError

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
        
        data[queryname] = dataset_dict

    client.service.logoff(sessionID=sessionid)
        
    return data


# PyOBIEE Exceptions
class PyObieeError(Exception):
    """PyObieeError class"""

class SAWRowsetSchemaError(PyObieeError):
    """SAWRowsetSchemaError is None, check your query and try again"""

class SAWRowsetDataError(PyObieeError):
    """SAWRowsetDataError is None, check your query and try again"""