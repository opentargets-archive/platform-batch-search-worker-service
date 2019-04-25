from envparse import env
from celery import Celery
import json
import requests
import time
from common.YAMLReader import YAMLReader

BROKER_URL = env('CELERY_BROKER_URL', default='redis://localhost:6379/0')
CELERY_RESULT_BACKEND = env('CELERY_RESULT_BACKEND', default='redis://localhost:6379/1')
CELERY_TASK_RESULT_EXPIRES = env('CELERY_TASK_RESULT_EXPIRES', default=14400)
CELERY_TRACK_STARTED = True
CELERY_SEND_EVENTS = True
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_SEND_SENT_EVENT = True
CELERY_SEND_EVENTS = True

# Create the app and set the broker location (RabbitMQ)
app = Celery("PlatformBatchSearchWorker",
             backend = CELERY_RESULT_BACKEND,
             broker = BROKER_URL)
    
def request_resouce(method,uri,args_parameters, headers):
    response = {}
    try:
        res = requests.request(method, headers= headers, url=uri, data=args_parameters)
        # The response is a JSON. If the response is different it will raise an exception.
        response = res.json()
    except Exception as ex:
        response = {'Content-Type':'application/json', 'status_code': '500', 'message': str(ex)}
    return response


def get_ot_target_api(yaml_dict, uri, url_prefix, args_parameters):
    url_root=uri+url_prefix
    if 'ot_target_api' in yaml_dict:
        url= url_root+yaml_dict.ot_target_api.uri
        method=yaml_dict.ot_target_api.method
        data_binary = yaml_dict.ot_target_api.params
        data_binary.id = [x.encode('UTF8') for x in args_parameters['target']]
        data_binary.size = len(data_binary.id)
        return request_resouce(method,url,json.dumps(data_binary),{'Content-Type': 'application/json'})
    return {'message': 'OpenTarget REST API not available (target endpoint)'}
        
def get_ot_target_enrich_api(yaml_dict, uri, url_prefix, args_parameters):
    url_root=uri+url_prefix
    if 'ot_target_enrichment_api' in yaml_dict:
        url= url_root+yaml_dict.ot_target_enrichment_api.uri
        method=yaml_dict.ot_target_enrichment_api.method
        data_binary = yaml_dict.ot_target_enrichment_api.params
        data_binary.target = [x.encode('UTF8') for x in args_parameters['target']]
        return request_resouce(method,url,json.dumps(data_binary), {'Content-Type': 'application/json'})
    return {'message': 'OpenTarget REST API not available (enrichment target endpoint)'}

def get_ot_evidence_filter_api(yaml_dict, uri, url_prefix, args_parameters):
    url_root=uri+url_prefix
    if 'ot_evidence_filter_api' in yaml_dict:
        url= url_root+yaml_dict.ot_evidence_filter_api.uri
        method=yaml_dict.ot_evidence_filter_api.method
        data_binary = yaml_dict.ot_evidence_filter_api.params
        data_binary.target = [x.encode('UTF8') for x in args_parameters['target']]
        return request_resouce(method,url,json.dumps(data_binary), {'Content-Type': 'application/json'})
    return {'message': 'OpenTarget REST API not available (enrichment target endpoint)'}
   
    
#Eg. --data-binary $'A2M-AS1\nKLF3-AS1\nMT-ND2' -> Header x-ndjson and NO json.dumps.
def get_uniprot(yaml_dict,list_symbols):
    if 'uniprot' in yaml_dict:
        url = yaml_dict.uniprot
        method=yaml_dict.uniprot.method
        data_binary = '\n'.join(list_symbols)  
        return request_resouce(method,url.uri,data_binary, {"Content-Type": "application/x-ndjson"})
    return {'message': 'UNIPROT resource not available'}

def uniprot_stats_info(uniprot):
    uniprot_stats = {}
    if 'summary' in uniprot:
        uniprot_stats['token'] = uniprot['summary']['token']
    if 'resourceSummary' in uniprot:
        for summary in uniprot['resourceSummary']:
            if summary['resource'] is not None and summary['resource'] == 'UNIPROT':
                uniprot_stats['num_pages']=summary['filtered']
    return uniprot_stats           

def get_uniprot_pagination(yaml_dict,uniprot_stats_info):
    num_pages=0 if 'num_pages' not in uniprot_stats_info else uniprot_stats_info['num_pages']
    token='' if 'token' not in uniprot_stats_info else uniprot_stats_info['token']
    if 'uniprot' in yaml_dict:
        url = yaml_dict.uniprot_pagination
        uri = url.uri.replace('pageSize=0','pageSize='+str(num_pages))
        uri = uri.replace('{token}',token)
        method=yaml_dict.uniprot_pagination.method
        data_binary = None
        return request_resouce(method,uri,data_binary, {"Content-Type": "application/json"})
    return {'message': 'UNIPROT resource not available - Pagination failed'}

def get_pathways(uniprot_complete):
    pathways_list = []
    if 'pathways' in uniprot_complete:
        for single_pathway in uniprot_complete['pathways']:
            pathways_list.append(single_pathway['stId'])
    return pathways_list

def get_biit_profile(yaml_dict,list_symbols):
    if 'biit_profile' in yaml_dict:
        url = yaml_dict.biit_profile
        method=yaml_dict.biit_profile.method
        data_binary = yaml_dict.biit_profile.params
        data_binary.query = [x.encode('UTF8') for x in list_symbols]
        return request_resouce(method,url.uri,json.dumps(data_binary), {"Content-Type": "application/json"})
    return {'message': 'BIIT resource not available'}

def get_partners_proteins(yaml_dict, list_uniprot_ids):
    if len(list_uniprot_ids) == 0:
        return []   
    # GET issue. Lenght of info related with 255 chars. Split the input.
    if 'partners_proteins' in yaml_dict:
        uri = yaml_dict.partners_proteins.uri
        protein_ids = ','.join(list_uniprot_ids)
        uri= uri.replace('{protein_ids}',protein_ids)
        method=yaml_dict.partners_proteins.method
        return request_resouce(method,uri,None, {"Content-Type": "application/json"})        
    return {'message': 'Partners proteins resource not available'}

def get_reactome_all(yaml_dict,uniprot_stats_info,pathways):
    token='' if 'token' not in uniprot_stats_info else uniprot_stats_info['token']
    if 'reactome_all' in yaml_dict:
        url = yaml_dict.reactome_all.uri.replace('{token}',token)
        method=yaml_dict.reactome_all.method
        data_binary = ",".join(pathways)
        return request_resouce(method,url,json.dumps(data_binary), {"Content-Type": "application/json"})
    return {'message': 'Reactome resource not available'}

def get_list_symbols_and_proteins(ot_target_api_response):
    symbols=[]
    uniprot = []
    if 'total' in ot_target_api_response:
        if ot_target_api_response['total'] > 0:
            for elem in ot_target_api_response['data']:
                symbols.append(elem['approved_symbol'])
                if elem['uniprot_id'] != "":
                    uniprot.append(elem['uniprot_id'])
    return {'symbols': symbols, 'uniprot': uniprot }
    

def get_time_of_execution(start):
    end = time.time()
    return end-start 

#data['Content-Type'] = ot_response.headers['Content-Type']
#data['status_code'] = ot_response.status_code
#data['state']= 'SUCCESS'       
@app.task(bind=True)
def run(self,uri,url_prefix,args_parameters):
    # POST a request using uri and parameter. The response should be JSON.
    #print('Executing task id {0.id}, args: {0.args!r} kwargs: {0.kwargs!r}'.format(
    #            self.request))
    task_id=self.request.id
    backend = app.backend
    backend.store_result(task_id, None, "STARTED 0/10")
    start_clock = time.time()
    complete_response = {}
    if 'target' not in args_parameters:
        complete_response = json.dumps({'error': 'Targets are mandatory'})
    try:
        yaml = YAMLReader()
        yaml_dict = yaml.read_yaml()
        ot_target_api_response = get_ot_target_api(yaml_dict, uri, url_prefix, args_parameters)
        list_symbols_and_uniprot_ids = get_list_symbols_and_proteins(ot_target_api_response)
        backend.store_result(task_id, "1/10", "STARTED")
        if len(list_symbols_and_uniprot_ids['symbols']) > 0:
            complete_response['targets'] = ot_target_api_response
            ot_target_enrich_api_response = get_ot_target_enrich_api(yaml_dict, uri, url_prefix, args_parameters)
            complete_response['targets_enrichment'] = ot_target_enrich_api_response
            backend.store_result(task_id, "2/10", "STARTED")
            uniprot=get_uniprot(yaml_dict,list_symbols_and_uniprot_ids['symbols'])
            complete_response['uniprot']=uniprot
            backend.store_result(task_id, "3/10", "STARTED")
            uniprot_stats=uniprot_stats_info(uniprot)
            backend.store_result(task_id, "4/10", "STARTED")
            evidence_filter=get_ot_evidence_filter_api(yaml_dict, uri, url_prefix, args_parameters)
            complete_response['evidence_filter']=evidence_filter
            backend.store_result(task_id, "5/10", "STARTED")
            uniprot_complete = get_uniprot_pagination(yaml_dict,uniprot_stats)
            complete_response['uniprot_complete']=uniprot_complete
            backend.store_result(task_id, "6/10", "STARTED")
            pathways=get_pathways(uniprot_complete)
            complete_response['pathways']=pathways
            backend.store_result(task_id, "7/10", "STARTED")
            biit=get_biit_profile(yaml_dict,list_symbols_and_uniprot_ids['symbols'])
            complete_response['biit']=biit
            backend.store_result(task_id, "8/10", "STARTED")
            partners_proteins=get_partners_proteins(yaml_dict,list_symbols_and_uniprot_ids['uniprot'])
            complete_response['partners_proteins']=partners_proteins
            backend.store_result(task_id, "9/10", "STARTED")
            reactome=get_reactome_all(yaml_dict,uniprot_stats,pathways)
            complete_response['reactome']=reactome
            backend.store_result(task_id, "10/10", "STARTED")
            complete_response['Content-Type'] = 'application/json'
            complete_response['status_code'] = 200
            complete_response['state']= 'SUCCESS'
            complete_response['exec_time'] = get_time_of_execution(start_clock)
            print complete_response['exec_time']
        else:        
            complete_response = {'Content-Type':'application/json', 'status_code': '500', 'message': 'No valid symbols', 'exec_time': get_time_of_execution(start_clock)}
    except Exception as ex:
        complete_response = {'Content-Type':'application/json', 'status_code': '500', 'message': str(ex), 'exec_time': get_time_of_execution(start_clock)}
    return json.dumps(complete_response)

@app.task
def ping():
    # This allows any producer to test this worker. 
    return json.dumps({'message': 'pong'})