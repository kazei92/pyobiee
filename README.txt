pyobiee is a custom connector for OBIEE (Oracle Business Intelligence Enterprise Edition).

It has two main functions:
1. Downloading a custom OBIEE report data (needs report's path)
2. Executing a SQL query and then downloading the data.

All that using SOAP protocol, Python SOAP client - zeep and OBIEE XMLViewService.

Links:
1. zeep - https://python-zeep.readthedocs.io/en/master/
2. OBIEE XMLViewService - https://docs.oracle.com/cd/E23943_01/bi.1111/e16364/methods.htm#BIEIT335

Data is returned in pandas DataFrame format to facilitate exporting and further processing.
