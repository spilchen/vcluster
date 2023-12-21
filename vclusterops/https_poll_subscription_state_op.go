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
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type httpsPollSubscriptionStateOp struct {
	opBase
	opHTTPSBase
	timeout int
}

func makeHTTPSPollSubscriptionStateOp(logger vlog.Printer, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) (httpsPollSubscriptionStateOp, error) {
	op := httpsPollSubscriptionStateOp{}
	op.name = "HTTPSPollSubscriptionStateOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword
	op.timeout = StartupPollingTimeout

	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}
	op.userName = userName
	op.httpsPassword = httpsPassword

	return op, nil
}

func (op *httpsPollSubscriptionStateOp) getPollingTimeout() int {
	// a negative value indicates no timeout and should never be used for this op
	return util.Max(op.timeout, 0)
}

func (op *httpsPollSubscriptionStateOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.Timeout = httpRequestTimeoutSeconds
		httpRequest.buildHTTPSEndpoint("subscriptions")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsPollSubscriptionStateOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsPollSubscriptionStateOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsPollSubscriptionStateOp) finalize(_ *opEngineExecContext) error {
	return nil
}

// The content of SubscriptionMap should look like
/* "subscription_list": [
	{
	  "node_name": "v_practice_db_node0001",
	  "shard_name": "replica",
	  "subscription_state": "ACTIVE",
	  "is_primary": true
	},
	{
	  "node_name": "v_practice_db_node0001",
	  "shard_name": "segment0001",
	  "subscription_state": "ACTIVE",
	  "is_primary": true
	},
	...
  ]
*/
type subscriptionList struct {
	SubscriptionList []subscriptionInfo `json:"subscription_list"`
}

type subscriptionInfo struct {
	Nodename          string `json:"node_name"`
	ShardName         string `json:"shard_name"`
	SubscriptionState string `json:"subscription_state"`
	IsPrimary         bool   `json:"is_primary"`
}

func (op *httpsPollSubscriptionStateOp) processResult(execContext *opEngineExecContext) error {
	err := pollState(op, execContext)
	if err != nil {
		return fmt.Errorf("not all subscriptions are ACTIVE, %w", err)
	}

	return nil
}

func (op *httpsPollSubscriptionStateOp) shouldStopPolling() (bool, error) {
	var subscriptList subscriptionList

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPasswordAndCertificateError(op.logger) {
			return true, fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			err := op.parseAndCheckResponse(host, result.content, &subscriptList)
			if err != nil {
				op.logger.PrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
				return true, err
			}

			// check whether all subscriptions are ACTIVE
			for _, s := range subscriptList.SubscriptionList {
				if s.SubscriptionState != "ACTIVE" {
					return false, nil
				}
			}

			op.logger.PrintInfo("All subscriptions are ACTIVE")
			return true, nil
		}
	}

	// this could happen if ResultCollection is empty
	op.logger.PrintError("[%s] empty result received from the provided hosts %v", op.name, op.hosts)
	return false, nil
}