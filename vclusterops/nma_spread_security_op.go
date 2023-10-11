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
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type nmaSpreadSecurityOp struct {
	OpBase
	catalogPathMap map[string]string
	keyType        string
}

type nmaSpreadSecurityPayload struct {
	CatalogPath           string `json:"catalog_path"`
	SpreadSecurityDetails string `json:"spread_security_details"`
}

const spreadKeyTypeVertica = "vertica"

// makeNMASpreadSecurityOp will create the op to set or rotate the key for
// spread encryption.
func makeNMASpreadSecurityOp(
	log vlog.Printer,
	keyType string,
) nmaSpreadSecurityOp {
	return nmaSpreadSecurityOp{
		OpBase: OpBase{
			log:   log,
			name:  "NMASpreadSecurityOp",
			hosts: nil, // We always set this at runtime from read catalog editor
		},
		catalogPathMap: nil, // Set at runtime after reading the catalog editor
		keyType:        keyType,
	}
}

func (op *nmaSpreadSecurityOp) setupRequestBody() (map[string]string, error) {
	if len(op.hosts) == 0 {
		return nil, fmt.Errorf("[%s] no hosts specified", op.name)
	}

	// Get the spread encryption key. Never write the contents of
	// securityDetails to a log or error messag. Otherwise, we risk leaking the
	// key.
	securityDetails, err := op.generateSecurityDetails()
	if err != nil {
		return nil, err
	}

	hostRequestBodyMap := make(map[string]string, len(op.hosts))
	for _, host := range op.hosts {
		fullCatalogPath, ok := op.catalogPathMap[host]
		if !ok {
			return nil, fmt.Errorf("could not find host %s in catalogPathMap %v", host, op.catalogPathMap)
		}
		payload := nmaSpreadSecurityPayload{
			CatalogPath:           getCatalogPath(fullCatalogPath),
			SpreadSecurityDetails: securityDetails,
		}
		// SPILLY - remove the security details
		op.log.Info("payload setup", "catalogPath", payload.CatalogPath, "securityDetails", securityDetails)

		dataBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("[%s] fail to marshal payload data into JSON string, detail %w", op.name, err)
		}
		// SPILLY - remove this log
		op.log.Info("mashaled payload", "payload", string(dataBytes))

		hostRequestBodyMap[host] = string(dataBytes)
	}
	return hostRequestBodyMap, nil
}

func (op *nmaSpreadSecurityOp) setupClusterHTTPRequest(hostRequestBodyMap map[string]string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest, len(hostRequestBodyMap))
	op.setVersionToSemVar()

	for host, requestBody := range hostRequestBodyMap {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("catalog/spread-security")
		httpRequest.RequestData = requestBody
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaSpreadSecurityOp) prepare(execContext *OpEngineExecContext) error {
	if err := op.setRuntimeParms(execContext); err != nil {
		return err
	}
	hostRequestBodyMap, err := op.setupRequestBody()
	if err != nil {
		return err
	}
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(hostRequestBodyMap)
}

func (op *nmaSpreadSecurityOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaSpreadSecurityOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *nmaSpreadSecurityOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			_, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				return errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}
	return nil
}

// setRuntimeParms will set options based on runtime context.
func (op *nmaSpreadSecurityOp) setRuntimeParms(execContext *OpEngineExecContext) error {
	// Always pull the hosts at runtime using the node with the latest catalog.
	// Need to use the ones with the latest catalog because those are the hosts
	// that we copy the spread.conf from during start db.
	op.hosts = execContext.hostsWithLatestCatalog

	op.catalogPathMap = make(map[string]string, len(op.hosts))
	err := updateCatalogPathMapFromCatalogEditor(op.hosts, &execContext.nmaVDatabase, op.catalogPathMap)
	if err != nil {
		return fmt.Errorf("failed to get catalog paths from catalog editor: %w", err)
	}
	return nil
}

func (op *nmaSpreadSecurityOp) generateSecurityDetails() (string, error) {
	keyID := op.generateKeyID()

	var spreadKey string
	var err error
	switch op.keyType {
	case spreadKeyTypeVertica:
		spreadKey, err = op.generateVerticaSpreadKey()
		if err != nil {
			return "", err
		}
	default:
		// Note, there is another key type that we support in the server
		// (aws-kms). But we haven't yet added support for that here.
		return "", fmt.Errorf("unsupported spread key type %s", op.keyType)
	}
	return fmt.Sprintf(`{%s: %s}`, keyID, spreadKey), nil
}

func (op *nmaSpreadSecurityOp) generateVerticaSpreadKey() (string, error) {
	const spreadKeySize = 32
	bytes := make([]byte, spreadKeySize)
	if _, err := crand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes for spread: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func (op *nmaSpreadSecurityOp) generateKeyID() string {
	const keyLength = 4
	var availChars = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]byte, keyLength)
	for i := range b {
		b[i] = availChars[rand.Intn(len(availChars))] //nolint:gosec
	}
	return string(b)
}
