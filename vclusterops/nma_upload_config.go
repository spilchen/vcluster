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
)

type NMAUploadConfigOp struct {
	OpBase
	catalogPathMap     map[string]string
	endpoint           string
	fileContent        *string
	hostRequestBodyMap map[string]string
}

type uploadConfigRequestData struct {
	CatalogPath string `json:"catalog_path"`
	Content     string `json:"content"`
}

func MakeNMAUploadConfigOp(
	opName string,
	nodeMap map[string]VCoordinationNode,
	newNodeHosts []string,
	endpoint string,
	fileContent *string,
) NMAUploadConfigOp {
	nmaUploadConfigOp := NMAUploadConfigOp{}
	nmaUploadConfigOp.name = opName
	nmaUploadConfigOp.endpoint = endpoint
	nmaUploadConfigOp.fileContent = fileContent
	nmaUploadConfigOp.catalogPathMap = make(map[string]string)
	nmaUploadConfigOp.hosts = newNodeHosts

	for _, host := range newNodeHosts {
		vnode, ok := nodeMap[host]
		if !ok {
			msg := fmt.Errorf("[%s] fail to get catalog path from host %s", opName, host)
			panic(msg)
		}
		nmaUploadConfigOp.catalogPathMap[host] = vnode.CatalogPath
	}

	return nmaUploadConfigOp
}

func (op *NMAUploadConfigOp) setupRequestBody(hosts []string) error {
	op.hostRequestBodyMap = make(map[string]string)

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

func (op *NMAUploadConfigOp) setupClusterHTTPRequest(hosts []string) {
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
}

func (op *NMAUploadConfigOp) Prepare(execContext *OpEngineExecContext) error {
	err := op.setupRequestBody(op.hosts)
	if err != nil {
		return err
	}
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMAUploadConfigOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAUploadConfigOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

func (op *NMAUploadConfigOp) processResult(execContext *OpEngineExecContext) error {
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
