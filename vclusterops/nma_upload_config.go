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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMAUploadConfigOp struct {
	OpBase
	catalogPathMap     map[string]string
	endpoint           string
	fileContent        *string
	hostRequestBodyMap map[string]string
	sourceConfigHost   []string
	destHosts          []string
	vdb                *VCoordinationDatabase
	encryptSpread      bool
}

type uploadConfigRequestData struct {
	CatalogPath string `json:"catalog_path"`
	Content     string `json:"content"`
}

// makeNMAUploadConfigOp sets up the input parameters from the user for the upload operation.
// To start the DB, insert a nil value for sourceConfigHost and newNodeHosts, and
// provide a list of database hosts for hosts.
// To create the DB, use the bootstrapHost value for sourceConfigHost, a nil value for newNodeHosts,
// and provide a list of database hosts for hosts.
// To add nodes to the DB, use the bootstrapHost value for sourceConfigHost, a list of newly added nodes
// for newNodeHosts and provide a nil value for hosts.
func makeNMAUploadConfigOp(
	log vlog.Printer,
	opName string,
	sourceConfigHost []string, // source host for transferring configuration files, specifically, it is
	// 1. the bootstrap host when creating the database
	// 2. the host with the highest catalog version for starting a database or starting nodes
	targetHosts []string, // list of hosts that need to be synchronized
	endpoint string,
	fileContent *string,
	vdb *VCoordinationDatabase,
	encryptSpread bool,
) NMAUploadConfigOp {
	nmaUploadConfigOp := NMAUploadConfigOp{}
	nmaUploadConfigOp.log = log
	nmaUploadConfigOp.name = opName
	nmaUploadConfigOp.endpoint = endpoint
	nmaUploadConfigOp.fileContent = fileContent
	nmaUploadConfigOp.catalogPathMap = make(map[string]string)
	nmaUploadConfigOp.sourceConfigHost = sourceConfigHost
	nmaUploadConfigOp.destHosts = targetHosts
	nmaUploadConfigOp.vdb = vdb
	nmaUploadConfigOp.encryptSpread = encryptSpread

	return nmaUploadConfigOp
}

func (op *NMAUploadConfigOp) setupRequestBody(hosts []string) error {
	op.hostRequestBodyMap = make(map[string]string)

	if op.encryptSpread {
		spreadKeyPayload := `{"y17b": "26169b33c812e9d1db67ec1dd3046a23219aa1e32840a105322de2dd06752279"}`
		// SPILLY - replace the spread key if it's already there
		*op.fileContent = fmt.Sprintf("%s\n# SPILLY added by me\n# VSpreadKey: %s", *op.fileContent, spreadKeyPayload)
		op.log.Info("modified spread conf", "contents", *op.fileContent)
	}

	for _, host := range hosts {
		uploadConfigData := uploadConfigRequestData{}
		uploadConfigData.CatalogPath = op.catalogPathMap[host]
		uploadConfigData.Content = *op.fileContent

		dataBytes, err := json.Marshal(uploadConfigData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *NMAUploadConfigOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint(op.endpoint)
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMAUploadConfigOp) prepare(execContext *OpEngineExecContext) error {
	op.catalogPathMap = make(map[string]string)
	// If any node's info is available, we set catalogPathMap from node's info.
	// This case is used for restarting nodes operation.
	// Otherwise, we set catalogPathMap from the catalog editor (start_db, create_db).
	if op.vdb == nil || len(op.vdb.HostNodeMap) == 0 {
		if op.sourceConfigHost == nil {
			//  if the host with the highest catalog version for starting a database or starting nodes is nil value
			// 	we identify the hosts that need to be synchronized.
			hostsWithLatestCatalog := execContext.hostsWithLatestCatalog
			if len(hostsWithLatestCatalog) == 0 {
				return fmt.Errorf("could not find at least one host with the latest catalog")
			}
			hostsNeedCatalogSync := util.SliceDiff(op.destHosts, hostsWithLatestCatalog)
			// Update the hosts that need to synchronize the catalog
			op.hosts = hostsNeedCatalogSync
			// If no hosts to upload, skip this operation. This can happen if all
			// hosts have the latest catalog.
			if len(op.hosts) == 0 {
				vlog.LogInfo("no hosts require an upload, skipping the operation")
				op.skipExecute = true
				return nil
			}
		} else {
			op.hosts = util.SliceDiff(op.destHosts, op.sourceConfigHost)
		}
		// Update the catalogPathMap for next upload operation's steps from information of catalog editor
		nmaVDB := execContext.nmaVDatabase
		err := updateCatalogPathMapFromCatalogEditor(op.hosts, &nmaVDB, op.catalogPathMap)
		if err != nil {
			return fmt.Errorf("failed to get catalog paths from catalog editor: %w", err)
		}
	} else {
		// use started nodes input provided by the user
		op.hosts = op.destHosts
		// Update the catalogPathMap for next upload operation's steps from node List information
		for host, vnode := range op.vdb.HostNodeMap {
			op.catalogPathMap[host] = getCatalogPath(vnode.CatalogPath)
		}
	}

	err := op.setupRequestBody(op.hosts)
	if err != nil {
		return err
	}
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMAUploadConfigOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAUploadConfigOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMAUploadConfigOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// the response object will be a dictionary including the destination of the config file, e.g.,:
			// {"destination":"/data/vcluster_test_db/v_vcluster_test_db_node0003_catalog/vertica.conf"}
			responseObj, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}
			_, ok := responseObj["destination"]
			if !ok {
				err = fmt.Errorf(`[%s] response does not contain field "destination"`, op.name)
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
