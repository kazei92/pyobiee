pyobiee is a custom connector for OBIEE (Oracle Business Intelligence Enterprise Edition).

It has two main functions (methods):
1. donwloadReport - Downloading data from a custom OBIEE report (needs report's path)
2. executeSQL - Executing a SQL query and then downloading its data.

All that using SOAP protocol, Python SOAP client - zeep and OBIEE XMLViewService.

Links:
1. zeep - https://python-zeep.readthedocs.io/en/master/
2. OBIEE XMLViewService - https://docs.oracle.com/cd/E23943_01/bi.1111/e16364/methods.htm#BIEIT335

Data is returned in pandas DataFrame format to facilitate exporting and further processing.
