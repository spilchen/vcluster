/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vclusterops

import (
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPSCreateNodesDepotOp struct {
	OpBase
	OpHTTPBase
	HostNodeMap map[string]VCoordinationNode
	DepotSize   string
}

// MakeHTTPSCreateNodesDepotOp will make an op that call vertica-http service to create depot for the new nodes
func MakeHTTPSCreateNodesDepotOp(vdb *VCoordinationDatabase, nodes []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSCreateNodesDepotOp {
	httpsCreateNodesDepotOp := HTTPSCreateNodesDepotOp{}
	httpsCreateNodesDepotOp.name = "HTTPSCreateNodesDepotOp"
	httpsCreateNodesDepotOp.hosts = nodes
	httpsCreateNodesDepotOp.useHTTPPassword = useHTTPPassword
	httpsCreateNodesDepotOp.HostNodeMap = vdb.HostNodeMap
	httpsCreateNodesDepotOp.DepotSize = vdb.DepotSize

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsCreateNodesDepotOp.userName = userName
	httpsCreateNodesDepotOp.httpsPassword = httpsPassword
	return httpsCreateNodesDepotOp
}

func (op *HTTPSCreateNodesDepotOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		node := op.HostNodeMap[host]
		httpRequest.BuildHTTPSEndpoint("nodes/" + node.Name + "/depot")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = map[string]string{"path": node.DepotPath}
		if op.DepotSize != "" {
			httpRequest.QueryParams["size"] = op.DepotSize
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSCreateNodesDepotOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSCreateNodesDepotOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSCreateNodesDepotOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	// every host needs to have a successful result, otherwise we fail this op
	// because we want depot created successfully on all hosts
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			success = false
			// not break here because we want to log all the failed nodes
			continue
		}

		/* decode the json-format response
		The successful response object will be a dictionary like below:
		{
			"node": "node01",
			"depot_location": "TMPDIR/create_depot/test_db/node01_depot"
		}
		*/
		resp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			vlog.LogPrintError(`[%s] fail to parse result on host %s, details: %s`, op.name, host, err)
			success = false
			// not break here because we want to log all the failed nodes
			continue
		}

		// verify if the node name and the depot location are correct
		if resp["node"] != op.HostNodeMap[host].Name || resp["depot_location"] != op.HostNodeMap[host].DepotPath {
			vlog.LogError(`[%s] should create depot %s on node %s, but created depot %s on node %s from host %s`,
				op.name, op.HostNodeMap[host].DepotPath, op.HostNodeMap[host].Name, resp["depot_location"], resp["node"], host)
			success = false
			// not break here because we want to log all the failed nodes
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPSCreateNodesDepotOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}