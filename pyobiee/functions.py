import time
import re
import pandas as pd
import xml.etree.ElementTree as ET
from .classes import *
import warnings
warnings.filterwarnings('ignore')


def get_schema(xml_view_service, query_type, path_or_sql):
    print("Trying to retrieve schema")
    for i in range(30):
        if query_type == "report":
            query_results = xml_view_service.execute_xml_query(report_ref=path_or_sql, output_format="SAWRowsetSchema")
        elif query_type == "sql":
            query_results = xml_view_service.execute_sql_query(sql=path_or_sql, output_format="SAWRowsetSchema")
        else:
            raise QueryError('Unknown query type')

        if query_results.rowset == None:
            print(f"Schema is empty, attempt {i+1}")
            time.sleep(6)
            continue
        else:
            break
    
    if query_results.rowset != None:
        headers = parse_schema(query_results.rowset)
        return headers
    else:
        raise QueryError('Schema is empty: execution timeout or there is a problem with your report')


def parse_schema(rowset):
    return re.findall(r'columnHeading="(.*?)"', rowset)


def get_rows(xml_view_service, query_type, path_or_sql, headers, report_params):
    print("Trying to retrieve data")
    for i in range(30):

        if query_type == "report":
            query_results = xml_view_service.execute_xml_query(report_ref=path_or_sql, output_format="SAWRowsetData", report_params=report_params)
        elif query_type == "sql":
            query_results = xml_view_service.execute_sql_query(sql=path_or_sql, output_format="SAWRowsetData")
        else:
            raise QueryError('Unknown query type')

        if query_results.rowset == None:
            print(f"Data rows are empty, attempt {i}")
            time.sleep(6)
            continue
        else:
            break
    

    data = parse_rows(query_results.rowset, headers)
    while (not query_results.finished):
        query_results = xml_view_service.fetch_next(query_id=query_results.queryID)
        batch = parse_rows(query_results.rowset, headers)
        print(f"Data batch size: { len(batch) } rows")
        data = data + batch
    
    return data


def parse_rows(rowset, headers):
    ETobject = ET.fromstring(rowset)
    rows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
    data = []
    for row in rows:
        row_data = []
        for i in range(len(headers)):
            value = row.find("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + str(i))
            if value != None:
                row_data.append(value.text)
            else:
                row_data.append(None)
        data.append(row_data)
    
    return data


def get_data(query_type, path_or_sql, wsdl, username, password, report_params = None, ignore_ssl = False):
    session_service = SAWSessionService(wsdl, ignore_ssl)
    session_id = session_service.logon(username, password)
    xml_view_service = XMLViewService(session_service, session_id)
    try:
        headers = get_schema(xml_view_service, query_type, path_or_sql)
        data = get_rows(xml_view_service, query_type, path_or_sql, headers, report_params)
    finally:
        session_service.logoff(session_id)

    return pd.DataFrame(data=data, columns=headers)


class PyObieeError(Exception):
    """PyObieeError class"""


class QueryError(PyObieeError):
    def __init__(self, message):
        self.message = message
