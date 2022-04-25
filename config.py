nodeColumList = ['nodeLabel', 'nodeName', 'timestamp']
edgeColumnList = ['nodeLabelFrom', 'nodeNameFrom', 'nodeLabelTo', 'nodeNameTo', 'timestamp']
tasks = {
        "DeleteNodes": {
            "query" : """mutation deleteNodesBulk ($var: [NodeInput!]!){
                        deleteNodeBulk(
                            input: $var
                        ) {
                            failureCount
                            successCount
                            nodes{
                            nodeId
                            nodeName
                            nodeLabel
                            timestamp
                            }
                            status
                        }
                        }""",
            "api" : "deleteNodeBulk"
        },
        "InsertNodes": {
            "query" : """mutation createNodesBulk( 
                    $var: [NodeInput!]!
                ){
            createNodeBulk (
                input: $var
            ) {
                failureCount
                successCount
                nodes{
                nodeId
                nodeName
                nodeLabel
                timestamp
                }
                status
            }
            }""",
            "api" : "createNodeBulk"
        },
        "UpdateNodes": {
            "query" : """mutation updateNodeBulk( 
                    $var: [NodeInput!]!
                ){
            updateNodeBulk (
                input: $var
            ) {
                failureCount
                successCount
                nodes{
                nodeId
                nodeName
                nodeLabel
                timestamp
                }
                status
            }
            }""",
            "api" : "updateNodeBulk"
        },
        "DeleteEdges": {
            "query" : """mutation deleteEdgesBulk ($var: [EdgeInput!]!){
                deleteEdgeBulk(
                    input: $var
                ) {
                    failureCount
                    successCount
                    edges{
                    edgeId
                    edgeLabelTo
                    nodeIdFrom
                    nodeLabelFrom
                    nodeNameFrom
                    nodeIdTo
                    nodeLabelTo
                    nodeNameTo                    
                    timestamp
                    }
                    status
                }
                }""",
            "api" : "deleteEdgeBulk"
        },
        "InsertEdges": {
            "query" : """mutation createEdgesBulk ($var: [EdgeInput!]!){
                createEdgeBulk(
                    input: $var
                ) {
                    failureCount
                    successCount
                    edges{
                    edgeId
                    edgeLabelTo
                    nodeIdFrom
                    nodeLabelFrom
                    nodeNameFrom
                    nodeIdTo
                    nodeLabelTo
                    nodeNameTo                    
                    timestamp
                    }
                    status
                }
                }""",
            "api" : "createEdgeBulk"
        },
        "UpdateEdges": {
            "query" : """mutation updateEdgesBulk ($var: [EdgeInput!]!){
                updateEdgeBulk(
                    input: $var
                ) {
                    failureCount
                    successCount
                    edges{
                        edgeId
                        edgeLabelTo
                        nodeIdFrom
                        nodeLabelFrom
                        nodeNameFrom
                        nodeIdTo
                        nodeLabelTo
                        nodeNameTo
                        properties
                        timestamp
                    }
                    status
                }
                }""",
            "api" : "createEdgeBulk"
        },
        "GetNodes": {
            "query" : """query queryBulkNode ( $var: [GetNodeInput!]! ) {
                getNodesBulk(
                    input: $var
                ){
                    failureCount
                    successCount
                    nodes {
                    nodeId,
                    nodeName,
                    nodeLabel,
                    properties{
                        name,
                        value,
                    }
                    timestamp
                    }
                }
                }""",
            "api" : "getNodesBulk"
        },
        "GetOneNode": {
            "query" : """query queryNode ( $var1: String!, $var2: String!) {
                                getNode(
                                    input: {
                                    nodeName: $var2,
                                    nodeLabel: $var1
                                    }
                                ){
                                    status
                                    statusMessage
                                    node {
                                    nodeId,
                                    nodeName,
                                    nodeLabel,
                                    properties{
                                        name,
                                        value,
                                    }
                                    timestamp
                                    }
                                }
                                }""",
            "api" : "getNode"
        },
        "GetEdges": {
            "query" : """query queryBulkEdges ($var : [GetEdgeInput!]!){
                            getEdgesBulk(
                                input: $var
                            ){
                                failureCount
                                successCount
                                edges {
                                edgeId,
                                edgeLabelTo,
                                nodeIdFrom,
                                nodeLabelFrom,
                                nodeNameFrom,
                                nodeIdTo,
                                nodeLabelTo,
                                nodeNameTo,                                
                                properties{
                                        name,
                                        value,
                                    },
                                timestamp
                                }
                            }
                    }""",
            "api" : "getEdgesBulk"
        },
        "GetOneEdge": {
            "query" : """query queryEdge ($var1: String!, $var2: String!, $var3: String!, $var4: String!, $var5: String! ){
                        getEdge(
                            input: {
                                nodeLabelFrom: $var1,
                                nodeNameFrom: $var2,
                                nodeLabelTo: $var3,
                                nodeNameTo: $var4,
                                edgeLabelTo: $var5
                            }
                        ){
                            status
                            statusMessage
                            edge {
                                edgeId,
                                edgeLabelTo,
                                nodeIdFrom,
                                nodeLabelFrom,
                                nodeNameFrom,                                
                                nodeIdTo,
                                nodeLabelTo,
                                nodeNameTo,                                
                                properties{
                                    name,
                                    value,
                                },
                                timestamp
                            }
                        }
                        }""",
            "api" : "getEdgesBulk"
        }
}