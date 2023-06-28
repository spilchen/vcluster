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

// VAddNodeOptions are the option arguments for the VAddNode API
type VAddNodeOptions struct {
	DatabaseOptions
	BootstrapHost  string // hostname or IP of an existing node in the cluster that is known to be UP
	SubclusterName string // Name of the subcluster that the new nodes will be added too
}

func (o *VAddNodeOptions) validateParseOptions() error {
	// SPILLY - code duplication smell with create db
	// batch 1: validate required parameters that need user input
	if *o.Name == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateDBName(*o.Name)
	if err != nil {
		return err
	}
	if len(o.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	if *o.CatalogPrefix == "" || !util.IsAbsPath(*o.CatalogPrefix) {
		return fmt.Errorf("must specify an absolute catalog path")
	}

	if *o.DataPrefix == "" || !util.IsAbsPath(*o.DataPrefix) {
		return fmt.Errorf("must specify an absolute data path")
	}

	// batch 2: validate required parameters with default values
	if o.Password == nil {
		o.Password = new(string)
		*o.Password = ""
		vlog.LogPrintInfoln("no password specified, using none")
	}

	// batch 3: validate other parameters
	if o.ConfigDirectory != nil {
		err := util.AbsPathCheck(*o.ConfigDirectory)
		if err != nil {
			return fmt.Errorf("must specify an absolute path for the config directory")
		}
	}

	if o.BootstrapHost == "" {
		return fmt.Errorf("must specify a bootstrap host")
	}

	if o.SubclusterName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}

	return nil
}

// analyzeOptions will modify some options based on what is chosen
func (o *VAddNodeOptions) analyzeOptions() error {
	// SPILLY - code duplication smell
	// resolve RawHosts to be IP addresses
	for _, host := range o.RawHosts {
		if host == "" {
			return fmt.Errorf("invalid empty host found in the provided host list")
		}
		addr, err := util.ResolveToOneIP(host, *o.Ipv6)
		if err != nil {
			return err
		}
		// use a list to respect user input order
		o.Hosts = append(o.Hosts, addr)
	}
	// process correct catalog path, data path and depot path prefixes
	*o.CatalogPrefix = util.GetCleanPath(*o.CatalogPrefix)
	*o.DataPrefix = util.GetCleanPath(*o.DataPrefix)
	*o.DepotPrefix = util.GetCleanPath(*o.DepotPrefix)
	return nil
}

func (o *VAddNodeOptions) validateAnalyzeOptions() error {
	if err := o.validateParseOptions(); err != nil {
		return err
	}
	if err := o.analyzeOptions(); err != nil {
		return err
	}
	return nil
}

// VAddNode is the top-level API for adding node(s) to an existing database.
func VAddNode(options *VAddNodeOptions) ([]VCoordinationNode, error) {
	if err := options.validateAnalyzeOptions(); err != nil {
		vlog.LogPrintError("add node options validation error, %w", err)
	}

	instructions, err := produceAddNodeInstructions(options)
	if err != nil {
		vlog.LogPrintError("fail to produce add node instructions, %w", err)
		return nil, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("fail to complete add node operation, %w", runError)
		return nil, runError
	}

	// SPILLY - return a VCoordinationNode for all of the nodes we create
	return nil, nil
}

// produceAddNodeInstructions will build a list of instructions to execute for
// the add node operation
func produceAddNodeInstructions(options *VAddNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// Some operations need all of the new hosts, plus the bootstrap host.
	// allHosts includes them all.
	allHosts := []string{
		options.BootstrapHost,
	}
	allHosts = append(allHosts, options.Hosts...)

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", allHosts)
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", allHosts, true)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
	)
	return instructions, nil
}
