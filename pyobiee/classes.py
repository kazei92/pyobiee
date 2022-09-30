from zeep import Client, Settings
from zeep.transports import Transport
from requests import Session


class SAWSessionService():

    def __init__(self, wsdl, ignore_ssl = False):
        session = Session()
        transport = Transport(session=session)
        
        if (ignore_ssl):
            transport.session.verify = False
        
        settings = Settings(xml_huge_tree=True)
        self.client = Client(wsdl=wsdl, transport=transport, settings=settings)
        self.service = self.client.service

    def logon(self, username, password):
        session_id = self.service.logon(name=username, password=password)
        return session_id

    def logoff(self, session_id):
        self.service.logoff(session_id)

    def get_current_user(self, session_id):
        return self.service.getCurUser(session_id)

    def get_session_environment(self, session_id):
        return self.service.getSessionEnvironment(session_id)

    def get_session_variable(self, names, session_id):
        return self.service.getSessionVariable(names, session_id)

    def impersonate(self, name, password, impersonate_id):
        session_id = self.service.getSessionVariable(name, password, impersonate_id)
        return session_id
    
    def keep_alive(self, session_id):
        self.service.keepAlive(session_id)
    
    def bind(self, service_name, port_name=None):
        return self.client.bind(service_name, port_name)


class XMLViewService():
    def __init__(self, session_service, session_id):
        self.service = session_service.client.bind('XmlViewService')
        self.session_id = session_id
        self.execution_options = {"async" : True, "maxRowsPerPage" : 10000, "refresh" : True, "presentationInfo" : True}
    
    def execute_sql_query(self, sql, output_format):
        query_results = self.service.executeSQLQuery(sql=sql, outputFormat=output_format, executionOptions=self.execution_options, 
                                                     sessionID=self.session_id)
        return query_results
    
    def execute_xml_query(self, report_ref, output_format, report_params=None):
        query_results = self.service.executeXMLQuery(report=report_ref, outputFormat=output_format, executionOptions=self.execution_options, 
                                                     sessionID=self.session_id, reportParams=report_params)
        return query_results

    def fetch_next(self, query_id):
        query_results = self.service.fetchNext(queryID=query_id, sessionID=self.session_id)
        return query_results

    def cancel_query(self, query_id, session_id):
        self.service.cancelQuery(query_id, session_id)

    def get_prompted_filters(self, report_ref, session_id):
        return self.service.getPromptedFilters(report_ref, session_id)


def get_available_services(wsdl):
    client = Client(wsdl=wsdl)
    service_names = []
    for service in client.wsdl.services.values():
        service_names.append(service.name)
    return service_names
