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

// vclusterops is a Go library to administer a Vertica cluster with HTTP RESTful
// interfaces. These interfaces are exposed through the Node Management Agent
// (NMA) and an HTTPS service embedded in the server. With this library you can
// perform administrator-level operations, including: creating a database,
// scaling up/down, restarting the cluster, and stopping the cluster.
package vclusterops

import (
	"fmt"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* Op and host http result status
 */

// ResultStatus is the data type for the status of
// ClusterOpResult and HostHTTPResult
type ResultStatus int

var wrongCredentialErrMsg = []string{"Wrong password", "Wrong certificate"}

const (
	SUCCESS   ResultStatus = 0
	FAILURE   ResultStatus = 1
	EXCEPTION ResultStatus = 2
)

const (
	GetMethod    = "GET"
	PutMethod    = "PUT"
	PostMethod   = "POST"
	DeleteMethod = "DELETE"
)

const (
	// track endpoint versions and the current version
	NMAVersion1    = "v1/"
	HTTPVersion1   = "v1/"
	NMACurVersion  = NMAVersion1
	HTTPCurVersion = HTTPVersion1
)

const (
	SuccessResult   = "SUCCESS"
	FailureResult   = "FAILURE"
	ExceptionResult = "FAILURE"
)

const (
	SuccessCode        = 200
	MultipleChoiceCode = 300
	UnauthorizedCode   = 401
	InternalErrorCode  = 500
)

// HostHTTPResult is used to save result of an Adapter's sendRequest(...) function
// it is the element of the adapter pool's channel
type HostHTTPResult struct {
	status     ResultStatus
	statusCode int
	host       string
	content    string
	err        error // This is set if the http response ends in a failure scenario
}

// The HTTP response with a 401 status code can have several scenarios:
// 1. Wrong password
// 2. Wrong certificate
// 3. The local node has not yet joined the cluster; the HTTP server will accept connections once the node joins the cluster.
// HTTPCheckDBRunningOp in create_db need to check all scenarios to see any HTTP running
// For HTTPSPollNodeStateOp in start_db, it requires only handling the first and second scenarios
func (hostResult *HostHTTPResult) IsUnauthorizedRequest() bool {
	return hostResult.statusCode == UnauthorizedCode
}

// check only password and certificate for start_db
func (hostResult *HostHTTPResult) IsPasswordandCertificateError() bool {
	if !hostResult.IsUnauthorizedRequest() {
		return false
	}
	resultString := fmt.Sprintf("%v", hostResult)
	for _, msg := range wrongCredentialErrMsg {
		if strings.Contains(resultString, msg) {
			vlog.LogError("the user has provided %s", msg)
			return true
		}
	}
	return false
}

func (hostResult *HostHTTPResult) IsInternalError() bool {
	return hostResult.statusCode == InternalErrorCode
}

func (hostResult *HostHTTPResult) IsHTTPRunning() bool {
	if hostResult.isPassing() || hostResult.IsUnauthorizedRequest() || hostResult.IsInternalError() {
		return true
	}
	return false
}

func (hostResult *HostHTTPResult) isPassing() bool {
	return hostResult.err == nil
}

func (hostResult *HostHTTPResult) isFailing() bool {
	return hostResult.status == FAILURE
}

func (hostResult *HostHTTPResult) isException() bool {
	return hostResult.status == EXCEPTION
}

// getStatusString converts ResultStatus to string
func (status ResultStatus) getStatusString() string {
	if status == FAILURE {
		return FailureResult
	} else if status == EXCEPTION {
		return ExceptionResult
	}
	return SuccessResult
}

/* Cluster ops interface
 */

// ClusterOp interface requires that all ops implements
// the following functions
// log* implemented by embedding OpBase, but overrideable
type ClusterOp interface {
	getName() string
	setupClusterHTTPRequest(hosts []string)
	Prepare(execContext *OpEngineExecContext) error
	Execute(execContext *OpEngineExecContext) error
	Finalize(execContext *OpEngineExecContext) error
	processResult(execContext *OpEngineExecContext) error
	logResponse(host string, result HostHTTPResult)
	logPrepare()
	logExecute()
	logFinalize()
	loadCertsIfNeeded(certs *HTTPSCerts, findCertsInOptions bool) error
}

/* Cluster ops basic fields and functions
 */

// OpBase defines base fields and implements basic functions
// for all ops
type OpBase struct {
	name               string
	hosts              []string
	clusterHTTPRequest ClusterHTTPRequest
	skipExecute        bool // This can be set during prepare if we determine no work is needed
}

type OpResponseMap map[string]string

func (op *OpBase) getName() string {
	return op.name
}

func (op *OpBase) parseAndCheckResponse(host, responseContent string, responseObj any) error {
	err := util.GetJSONLogErrors(responseContent, &responseObj, op.name)
	if err != nil {
		vlog.LogError("[%s] fail to parse response on host %s, detail: %s", op.name, host, err)
		return err
	}
	vlog.LogInfo("[%s] JSON response from %s is %+v\n", op.name, host, responseObj)

	return nil
}

func (op *OpBase) parseAndCheckMapResponse(host, responseContent string) (OpResponseMap, error) {
	var responseObj OpResponseMap
	err := op.parseAndCheckResponse(host, responseContent, &responseObj)

	return responseObj, err
}

func (op *OpBase) setVersionToSemVar() {
	op.clusterHTTPRequest.SemVar = SemVer{Ver: "1.0.0"}
}

// TODO: implement another parse function for list response

func (op *OpBase) logResponse(host string, result HostHTTPResult) {
	vlog.LogPrintInfo("[%s] result from host %s summary %s, details: %+v",
		op.name, host, result.status.getStatusString(), result)
}

func (op *OpBase) logPrepare() {
	vlog.LogInfo("[%s] Prepare() called\n", op.name)
}

func (op *OpBase) logExecute() {
	vlog.LogInfo("[%s] Execute() called\n", op.name)
}

func (op *OpBase) logFinalize() {
	vlog.LogInfo("[%s] Finalize() called\n", op.name)
}

func (op *OpBase) execute(execContext *OpEngineExecContext) error {
	err := execContext.dispatcher.sendRequest(&op.clusterHTTPRequest)
	if err != nil {
		vlog.LogError("Fail to dispatch request %v", op.clusterHTTPRequest)
		return err
	}
	return nil
}

// if found certs in the options, we add the certs to http requests of each instruction
func (op *OpBase) loadCertsIfNeeded(certs *HTTPSCerts, findCertsInOptions bool) error {
	if !findCertsInOptions {
		return nil
	}
	// Can skip loading of certs if not going to perform an execute
	if op.skipExecute {
		return nil
	}

	// this step is executed after Prepare() so all http requests should be set up
	if len(op.clusterHTTPRequest.RequestCollection) == 0 {
		return fmt.Errorf(fmt.Sprintf("[%s] has not set up a http request, skipping cert loading", op.name))
	}

	for host := range op.clusterHTTPRequest.RequestCollection {
		request := op.clusterHTTPRequest.RequestCollection[host]
		request.UseCertsInOptions = true
		request.Certs.key = certs.key
		request.Certs.cert = certs.cert
		request.Certs.caCert = certs.caCert
		op.clusterHTTPRequest.RequestCollection[host] = request
	}
	return nil
}

/* Sensitive fields in request body
 */
type SensitiveFields struct {
	DBPassword         string `json:"db_password"`
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
}

func (maskedData *SensitiveFields) maskSensitiveInfo() {
	const maskedValue = "******"

	maskedData.DBPassword = maskedValue
	maskedData.AWSAccessKeyID = maskedValue
	maskedData.AWSSecretAccessKey = maskedValue
}

/* Cluster HTTPS ops basic fields
 * which are needed for https requests using password auth
 * specify whether to use password auth explicitly
 * for the case where users do not specify a password, e.g., create db
 * we need the empty password "" string
 */
type OpHTTPBase struct {
	useHTTPPassword bool
	httpsPassword   *string
	userName        string
}

// we may add some common functions for OpHTTPBase here

// VClusterCommands is for vcluster-ops library user to do mocking test in their program
// The user can mock VCreateDatabase, VStopDatabase ... in their unit tests
type VClusterCommands struct {
}
