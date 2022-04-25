import pandas as pd
from pyspark.sql import SparkSession
import io
import os
import datetime
import pyarrow.parquet as pq
import requests
import json
import sys
from config import tasks, nodeColumList, edgeColumnList

#Code for testing on dev desktop
url = 'http://zshekhar-al2.aka.corp.amazon.com:4000/'

def deleteGraph():
    query = """mutation deletegraph {
              deleteGraph
            }"""
    try:
        resp = requests.post(url, json={'query': query})
        r = json.loads(resp.content.decode('utf8'))['data']['getNode']
        return {status : r['status'], nodeId: r['node']['nodeId']}
    #r = json.loads(resp.content.decode('utf8'))
    except Exception as e:
        return None



def getNodeDetails(nodelabel, nodename):
    task_details = tasks['GetOneNode']
    query = task_details['query']
    api = task_details['api']
    variables = { "var1" : nodelabel, "var2" : nodename }
    try:
        resp = requests.post(url, json={'query': query, "variables": variables})
        r = json.loads(resp.content.decode('utf8'))['data']['getNode']
        if r['status'] == 'SUCCESS':
            return {status : r['status'], nodeId: r['node']['nodeId']}
        else:
            return {status: r['status'], nodeId: None}
    #r = json.loads(resp.content.decode('utf8'))
    except Exception as e:
        return None


def getEdgeDetails(nodelabelfrom, nodenamefrom, nodelabelto, nodenameto, edgelabelto):
    task_details = tasks['GetOneEdge']
    query = task_details['query']
    api = task_details['api']
    variables = { "var1" : nodelabelfrom, "var2" : nodenamefrom, "var3" : nodelabelto, "var4" : nodenameto, "var5" : edgelabelto }
    try:
        resp = requests.post(url, json={'query': query, "variables": variables})
        r = json.loads(resp.content.decode('utf8'))['data']['getEdge']
        if r['status'] == 'SUCCESS':
            return {status : r['status'], edgeId: r['edge']['edgeId']}
        else:
            return {status : r['status'], edgeId: None}
    #r = json.loads(resp.content.decode('utf8'))
    except Exception as e:
        return None



def filterEdgesDfForNodes(dfE, dfN):
    Nodes = dfN.select("nodelabel").distinct()
    print(dfE.columns)
    dfE = dfE.join(Nodes,(dfE.nodelabelfrom == Nodes.nodelabel)).select(dfE['nodelabelfrom'], dfE['nodenamefrom'] ,dfE['nodelabelto'], dfE['nodenameto'], dfE['edgelabelto'],dfE['properties'],dfE['timestamp'])
    dfE = dfE.join(Nodes,(dfE.nodelabelto == Nodes.nodelabel)).select(dfE['nodelabelfrom'], dfE['nodenamefrom'] ,dfE['nodelabelto'], dfE['nodenameto'], dfE['edgelabelto'],dfE['properties'],dfE['timestamp'])
    dfE = dfE.join(dfN,(dfE.nodelabelto == dfN.nodelabel) & (dfE.nodenameto == dfN.nodename)).select(dfE['nodelabelfrom'], dfE['nodenamefrom'] ,dfE['nodelabelto'], dfE['nodenameto'], dfE['edgelabelto'],dfE['properties'],dfE['timestamp'])
    dfE = dfE.join(dfN,(dfE.nodelabelfrom == dfN.nodelabel) & (dfE.nodenamefrom == dfN.nodename)).select(dfE['nodelabelfrom'], dfE['nodenamefrom'] ,dfE['nodelabelto'], dfE['nodenameto'], dfE['edgelabelto'],dfE['properties'],dfE['timestamp'])
    print(dfE.columns)
    return dfE


def bulkOperations(input_list, task):
    try:
        task_details = tasks[task]
        query = task_details['query']
        api = task_details['api']
    except Exception as e:
        print('Task Fetch Error: {}'.format(e))
        raise e
    variables = { "var" : input_list}
    resp = requests.post(url, json={'query': query, "variables": variables})
    r = json.loads(resp.content.decode('utf8'))
    # if task == 'GetEdges':
    #     print(r)
    returnData = r['data'][api]
    returnData['task'] = task
    return returnData
    # if task == 'GetNodes':
    #     return { 'task': task, 'nodes': r['data'][api]['nodes'], 'failureCount': r['data'][api]['failureCount'], 'successCount': r['data'][api]['successCount'] }
    # elif task == 'GetEdges':
    #     print(r)
    #     return { 'task': task, 'edges': r['data'][api]['edges'], 'failureCount': r['data'][api]['failureCount'], 'successCount': r['data'][api]['successCount'] }        
    # else:
    #     #Check how many failed??
    #     #print('Task-> {}, Response -> {}, status -> {}, failureCount -> {}, successCount -> {}'. format(task, resp.status_code, r['data']['createNodeBulk']['status'], r['data']['createNodeBulk']['failureCount'], r['data']['createNodeBulk']['successCount']))
    #     print({ 'task': task, 'status': r['data'][api]['status'], 'failureCount': r['data'][api]['failureCount'], 'successCount': r['data'][api]['successCount'] })
    #     return { 'status': r['data'][api]['status'], 'failureCount': r['data'][api]['failureCount'], 'successCount': r['data'][api]['successCount'] }




def fetchDetails(opType, dict_row):
    try:
        if opType == 'node':
            nodelabel = dict_row['nodelabel']
            nodename = dict_row['nodename']
            properties = json.loads(dict_row['properties'])
            timestamp = int(dict_row['timestamp'])
            return {'nodeLabel': nodelabel, 'nodeName': nodename, 'properties': properties, 'timestamp': timestamp}
        else:
            nodelabelfrom = dict_row['nodelabelfrom']
            nodenamefrom = dict_row['nodenamefrom']
            nodelabelto = dict_row['nodelabelto']
            nodenameto = dict_row['nodenameto']
            edgelabelto = dict_row['edgelabelto']
            properties = json.loads(dict_row['properties'])
            timestamp = int(dict_row['timestamp'])
            return {'nodeLabelFrom': nodelabelfrom, 'nodeNameFrom': nodenamefrom, 'nodeLabelTo': nodelabelto, 'nodeNameTo': nodenameto, 'edgeLabelTo': edgelabelto, 'properties': properties, 'timestamp': timestamp}
    except Exception as e:
        raise e




def removeDictFromList(properties):
    i=0
    for rec in properties:
        if rec['name'] in ['source','nodeId','edgeId','nodeFromId','nodeToId','edgeLabel','last_updated_timestamp']:
            del properties[i]
        i=i+1
    return properties




def getPandasDfFromParquetFile(file):
    df = pd.read_parquet(file, engine='pyarrow')
    return df


def getRecordsCategorised(record_list, existingRecords, recordType):
    createList = []; updateList = []; skipList = []
    for record in record_list:
        found = False
        for nodeOrEdge in existingRecords:
            #If bulk loaded, the node's properties seems to have additional property as { name: source, value: bulk}
            # This property causes mismatch
            if recordType == 'edge':
                print(nodeOrEdge)
            nodeOrEdge['properties'] = removeDictFromList(nodeOrEdge['properties'])
            #Evaluate Task  
            if recordType == 'node':  
                if record['nodeLabel'] == nodeOrEdge['nodeLabel'] and record['nodeName'] == nodeOrEdge['nodeName']:
                    if sorted(record['properties'], key = lambda i: i['name']) != sorted(nodeOrEdge['properties'], key = lambda i: i['name']):
                        updateList.append(record)
                    else:
                        skipList.append(record)
                    found = True
                    break
                else:
                    found = False
            elif recordType == 'edge': 
                if record['nodeLabelFrom'] == nodeOrEdge['nodeLabelFrom'] and record['nodeNameFrom'] == nodeOrEdge['nodeNameFrom'] and record['nodeLabelTo'] == nodeOrEdge['nodeLabelTo'] and record['nodeNameTo'] == nodeOrEdge['nodeNameTo'] and record['edgeLabelTo'] == nodeOrEdge['edgeLabelTo']:
                    if sorted(record['properties'], key = lambda i: i['name']) != sorted(nodeOrEdge['properties'], key = lambda i: i['name']):
                        updateList.append(record)
                    else:
                        skipList.append(record)
                    found = True
                    break
                else:
                    found = False
        if found == False:
            createList.append(record)
    return { 
        "C": createList,
        "U": updateList,
        "S": skipList
    }


# def getNodeRecordsCategorised(record_list, existingNodes):
#     createList = []; updateList = []; skipList = []
#     for record in record_list:
#         found = False
#         for node in existingNodes:
#             #If bulk loaded, the node's properties seems to have additional property as { name: source, value: bulk}
#             # This property causes mismatch
#             node['properties'] = removeDictFromList(node['properties'])
#             #Evaluate Task    
#             if record['nodeLabel'] == node['nodeLabel'] and record['nodeName'] == node['nodeName']:
#                 if sorted(record['properties'], key = lambda i: i['name']) != sorted(node['properties'], key = lambda i: i['name']):
#                     updateList.append(record)
#                 else:
#                     skipList.append(record)
#                 found = True
#                 break
#             else:
#                 found = False
#         if found == False:
#             createList.append(record)
#     return { 
#         "C": createList,
#         "U": updateList,
#         "S": skipList
#     }


# def getEdgeRecordsCategorised(record_list, existingEdges):
#     createList = []; updateList = []; skipList = []
#     for record in record_list:
#         found = False
#         for edge in existingEdges:
#             edge['properties'] = removeDictFromList(edge['properties'])
#             #Evaluate Task    
#             if record['nodeLabelFrom'] == node['nodeLabelFrom'] and record['nodeNameFrom'] == node['nodeNameFrom'] and record['nodeLabelTo'] == node['nodeLabelTo'] and record['nodeNameTo'] == node['nodeNameTo'] and record['edgeLabelTo'] == node['edgeLabelTo']:
#                 if sorted(record['properties'], key = lambda i: i['name']) != sorted(node['properties'], key = lambda i: i['name']):
#                     updateList.append(record)
#                 else:
#                     skipList.append(record)
#                 found = True
#                 break
#             else:
#                 found = False
#         if found == False:
#             createList.append(record)
#     return { 
#         "C": createList,
#         "U": updateList,
#         "S": skipList
#     }


def parseEdgeOrNodeRecord(nodeOredge, opType):
    if opType == 'node':
        return {'nodeLabel': nodeOredge['nodeLabel'], 'nodeName': nodeOredge['nodeName'], 'timestamp': nodeOredge['timestamp'] }
    return {'edgeLabelTo': nodeOredge['edgeLabel'], 'nodeLabelFrom': nodeOredge['nodeLabelFrom'], 'nodeNameFrom': nodeOredge['nodeNameFrom'], 'nodeLabelTo': nodeOredge['nodeLabelTo'], 'nodeNameTo': nodeOredge['nodeNameTo'], 'timestamp': nodeOredge['timestamp'] }


#Compare success records with original List
#Find the once which failed OrigList Minus SuccessList
#Return the failed List
def getListForRetry(origList, successList, opType):
    returnList = []
    successNodeOrEdgeList = []
    for nodeOrEdge in successList:
        successNodeOrEdgeList.append(parseEdgeOrNodeRecord(nodeOrEdge, opType))
    for nodeOrEdge in origList:
        nodeOrEdgeCols = parseEdgeOrNodeRecord(nodeOrEdge, opType)
        if nodeOrEdgeCols in successNodeOrEdgeList:
            continue
        else:
            returnList.append(nodeOrEdge)
    return returnList


def bulkCreateOrUpdate(createOrUpdateList, op, opType):
    attempt_retry = True; attempt_counter = 0; max_counter = 2
    while attempt_retry:
        #op_resp = bulkOperations(updateList, 'UpdateNodes' if opType == 'node' else 'UpdateEdges')
        op_resp = bulkOperations(createOrUpdateList, op)
        print({ 'op': op, 'task': op_resp['task'], 'status': op_resp['status'], 'failureCount': op_resp['failureCount'], 'successCount': op_resp['successCount'] })
        attempt_counter = attempt_counter + 1
        if op_resp['failureCount'] == 0 or attempt_counter == max_counter:
            attempt_retry = False
        else:
            print('Failed {} task, Records Failed: {}, Retry Attempt: {}'.format(op,op_resp['failureCount'],attempt_counter))
            #updateList = getListForRetry(updateList, op_resp['nodes' if opType == 'node' else 'edges'], opType)  
            createOrUpdateList = getListForRetry(createOrUpdateList, op_resp['nodes' if opType == 'node' else 'edges'], opType)
    if attempt_counter == max_counter:
        print('Retries Exhausted for {}.....'.format(op))


def processData(df, batch_size, opType):
    total_record_count = df.count()
    record_list = []; searchRecordList = []
    cntr = 0
    if total_record_count > 0:
        for row in df.rdd.toLocalIterator():
            try:
                dict_row = row.asDict()
                if dict_row.get('properties',None) != None:
                    total_record_count = total_record_count - 1
                    dtls = fetchDetails(opType, dict_row)
                    record_list.append(dtls)
                    searchRecordList.append({'nodeLabel': dtls['nodeLabel'], 'nodeName': dtls['nodeName'] }) if opType == 'node' else searchRecordList.append({'nodeLabelFrom': dtls['nodeLabelFrom'], 'nodeNameFrom': dtls['nodeNameFrom'], 'nodeLabelTo': dtls['nodeLabelTo'], 'nodeNameTo': dtls['nodeNameTo'], 'edgeLabelTo': dtls['edgeLabelTo'] })
                if len(record_list) == batch_size or total_record_count == 0:
                    print('Searching....')
                    response = bulkOperations(searchRecordList, 'GetNodes' if opType == 'node' else 'GetEdges')
                    createList = []; updateList = []; skipList = []
                    #resp = getNodeRecordsCategorised(record_list, response['nodes']) if opType == 'node' else getEdgeRecordsDistributed(record_list, response['edges'])
                    #getRecordsCategorised(record_list, existingRecords, recordType)
                    resp = getRecordsCategorised(record_list, response['nodes'] if opType == 'node' else response['edges'], opType)
                    createList = resp['C']; updateList = resp['U']; skipList = resp['S']
                    print('opType: {}, failureCount: {}, successCount: {}, createListCount: {}, updateListCount: {}, skipListCount: {}'.format(opType, response['failureCount'], response['successCount'], len(createList), len(updateList), len(skipList)))
                    #If valid records for Update, then Update Nodes
                    if len(updateList) > 0:
                        print('Updating....')
                        bulkCreateOrUpdate(updateList, 'UpdateNodes' if opType == 'node' else 'UpdateEdges', opType)
                    #If valid records for Creation, then Creation Nodes    
                    if len(createList) > 0:
                        print('Creating....')
                        #op_resp = bulkOperations(createList, 'InsertNodes' if opType == 'node' else 'InsertEdges')
                        bulkCreateOrUpdate(createList, 'InsertNodes' if opType == 'node' else 'InsertEdges', opType)
                        #print({ 'task': op_resp['task'], 'status': op_resp['status'], 'failureCount': op_resp['failureCount'], 'successCount': op_resp['successCount'] })
                    record_list = []; searchRecordList = []; createList = []; updateList = []; skipList = []
            except Exception as e:
                #print(dict_row)
                raise e
    else:
        print('No {} data to process...'.format(opType))



################## Main ####################
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
cntr = 0
for file in ['/home/ankp/nodes_data/nodes0000_part_00.parquet', '/home/ankp/nodes_data/nodes0001_part_00.parquet', '/home/ankp/nodes_data/nodes0002_part_00.parquet', '/home/ankp/nodes_data/nodes0003_part_00.parquet', '/home/ankp/nodes_data/nodes0004_part_00.parquet','/home/ankp/nodes_data/nodes0005_part_00.parquet']:
    if cntr == 0:
        nodesDf = spark.createDataFrame(getPandasDfFromParquetFile(file))
    else:
        nodesDf = nodesDf.unionAll(spark.createDataFrame(getPandasDfFromParquetFile(file)))
    cntr = cntr + 1
print('Nodes Count: {}'.format(nodesDf.count()))
cntr = 0

for file in ['/home/ankp/edges_data/edges0000_part_00.parquet', '/home/ankp/edges_data/edges0001_part_00.parquet', '/home/ankp/edges_data/edges0002_part_00.parquet', '/home/ankp/edges_data/edges0003_part_00.parquet', '/home/ankp/edges_data/edges0004_part_00.parquet', '/home/ankp/edges_data/edges0005_part_00.parquet']:
    if cntr == 0:
        edgesDf = spark.createDataFrame(getPandasDfFromParquetFile(file))
    else:
        edgesDf = edgesDf.unionAll(spark.createDataFrame(getPandasDfFromParquetFile(file)))
    cntr = cntr + 1
print('Edges Count: {}'.format(edgesDf.count()))
#Filtering
edgesDf = filterEdgesDfForNodes(edgesDf, nodesDf)
print('Filtered Edges Count: {}'.format(edgesDf.count()))
processData(nodesDf, 1000, 'node')
processData(edgesDf, 1000, 'edge')
# processData(['/home/ankp/nodes0000_part_00.parquet'], 'node', 'cdc')
# processData(['/home/ankp/nodes0000_part_00.parquet'], 'edge')  