# Installation
```pip install pyobiee```

Dependencies:
requests,
zeep - SOAP client, 
pandas (only to facilitate integration with Power BI)

# General Information
pyobiee is a wrapper for OBIEE (Oracle Business Intelligence Enterprise Edition) SOAP.
Tested on OBIEE 12C and python 3.8.5

# How to use
```
import pyobiee
dataframe = pyobiee.get_data(query_type, path_or_sql, wsdl, username, password)
```

Arguments:
1. query_type: string, possible values are 'sql' or 'report'
2. path_or_sql: string, depending on query_type either:
* a report path inside OBIEE catalog, example '\somefolder\somesubfolder\ReportName'
* a sql query, example 'SELECT * FROM dbo.SubjectArea.DataColumn'
3. wsdl:string, wsdl url of the web service, example: 'http://host:port/analytics-ws/saw.dll/wsdl/v7'
4. username: string, example 'IvanIvanovich'
5. password: string, example '28QSAfsqs'

# Documentation
https://github.com/kazei92/pyobiee/wiki/

Links:
OBIEE Docs - https://docs.oracle.com/middleware/1221/biee/BIEIT/soa_overview.htm#BIEIT3171
