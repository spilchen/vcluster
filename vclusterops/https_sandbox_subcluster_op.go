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
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
)

type httpsSandboxingOp struct {
	opBase
	opHTTPSBase
	hostRequestBodyMap map[string]string
	scName             string
	sandboxName        string
}

// This op is used to sandbox the given subcluster `scName` as `sandboxName`
func makeHTTPSandboxingOp(scName, sandboxName string,
	useHTTPPassword bool, userName string, httpsPassword *string) (httpsSandboxingOp, error) {
	op := httpsSandboxingOp{}
	op.name = "HTTPSSansboxingOp"
	op.useHTTPPassword = useHTTPPassword
	op.scName = scName
	op.sandboxName = sandboxName

	if useHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
		if err != nil {
			return op, err
		}

		op.userName = userName
		op.httpsPassword = httpsPassword
	}

	return op, nil
}

func (op *httpsSandboxingOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint(Subclstr + op.scName + "/sandbox")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.hostRequestBodyMap
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsSandboxingOp) setupRequestBody() error {
	op.hostRequestBodyMap = make(map[string]string)
	op.hostRequestBodyMap["sandbox"] = op.sandboxName

	return nil
}

func (op *httpsSandboxingOp) prepare(execContext *opEngineExecContext) error {
	if len(execContext.sandboxingHosts) == 0 {
		return fmt.Errorf(`[%s] Cannot find any up hosts in OpEngineExecContext`, op.name)
	}
	// use shortlisted hosts to execute https post request, this host/hosts will be the initiator
	hosts := execContext.sandboxingHosts
	err := op.setupRequestBody()
	if err != nil {
		return err
	}
	execContext.dispatcher.setup(hosts)

	return op.setupClusterHTTPRequest(hosts)
}

func (op *httpsSandboxingOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsSandboxingOp) processResult(_ *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isUnauthorizedRequest() {
			// skip checking response from other nodes because we will get the same error there
			return result.err
		}
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			// try processing other hosts' responses when the current host has some server errors
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary:
		/*
			{
			  "detail": ""
			}
		*/
		_, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			return fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
		}

		return nil
	}

	return allErrs
}

func (op *httpsSandboxingOp) finalize(_ *opEngineExecContext) error {
	return nil
}
