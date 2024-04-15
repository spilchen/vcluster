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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VAddNodeOptions represents the available options for VAddNode.
type VAddNodeOptions struct {
	DatabaseOptions
	// Hosts to add to database
	NewHosts []string
	// Name of the subcluster that the new nodes will be added to
	SCName string
	// A primary up host that will be used to execute add_node operations
	Initiator string
	// Depot size, e.g., 10G
	DepotSize string
	// Skip rebalance shards if true
	SkipRebalanceShards *bool
	// Use force remove if true
	ForceRemoval bool
	// If the path is set, the NMA will store the Vertica start command at the path
	// instead of executing it. This is useful in containerized environments where
	// you may not want to have both the NMA and Vertica server in the same container.
	// This feature requires version 24.2.0+.
	StartUpConf string
	// Names of the existing nodes in the cluster. This option can be
	// used to remove partially added nodes from catalog.
	ExpectedNodeNames []string
}

func VAddNodeOptionsFactory() VAddNodeOptions {
	opt := VAddNodeOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (o *VAddNodeOptions) setDefaultValues() {
	o.DatabaseOptions.setDefaultValues()

	o.SkipRebalanceShards = new(bool)
}

func (o *VAddNodeOptions) validateEonOptions() error {
	if o.DepotPrefix != "" {
		return util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
	}
	return nil
}

func (o *VAddNodeOptions) validateExtraOptions() error {
	// data prefix
	if o.DataPrefix != "" {
		return util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
	}
	return nil
}

func (o *VAddNodeOptions) validateParseOptions(logger vlog.Printer) error {
	// batch 1: validate required parameters
	err := o.validateBaseOptions("db_add_node", logger)
	if err != nil {
		return err
	}

	// batch 2: validate all other params
	return o.validateExtraOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (o *VAddNodeOptions) analyzeOptions() (err error) {
	o.NewHosts, err = util.ResolveRawHostsToAddresses(o.NewHosts, o.IPv6)
	if err != nil {
		return err
	}

	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	// resolve RawHosts to be IP addresses
	if len(o.RawHosts) > 0 {
		o.Hosts, err = util.ResolveRawHostsToAddresses(o.RawHosts, o.IPv6)
		if err != nil {
			return err
		}
		o.normalizePaths()
	}

	return nil
}

func (o *VAddNodeOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	err := o.validateParseOptions(logger)
	if err != nil {
		return err
	}

	return o.analyzeOptions()
}

// VAddNode adds one or more nodes to an existing database.
// It returns a VCoordinationDatabase that contains catalog information and any error encountered.
func (vcc VClusterCommands) VAddNode(options *VAddNodeOptions) (VCoordinationDatabase, error) {
	vdb := makeVCoordinationDatabase()

	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return vdb, err
	}

	err = vcc.getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return vdb, err
	}

	err = options.completeVDBSetting(&vdb)
	if err != nil {
		return vdb, err
	}

	if vdb.IsEon {
		// checking this here because now we have got eon value from
		// the running db.
		if e := options.validateEonOptions(); e != nil {
			return vdb, e
		}
	}

	err = options.setInitiator(vdb.PrimaryUpNodes)
	if err != nil {
		return vdb, err
	}

	// trim stale node information from catalog
	// if NodeNames is provided
	err = vcc.trimNodesInCatalog(&vdb, options)
	if err != nil {
		return vdb, err
	}

	// add_node is aborted if requirements are not met.
	// Here we check whether the nodes being added already exist
	err = checkAddNodeRequirements(&vdb, options.NewHosts)
	if err != nil {
		return vdb, err
	}

	err = vdb.addHosts(options.NewHosts, options.SCName)
	if err != nil {
		return vdb, err
	}

	instructions, err := vcc.produceAddNodeInstructions(&vdb, options)
	if err != nil {
		return vdb, fmt.Errorf("fail to produce add node instructions, %w", err)
	}

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.run(vcc.Log); runError != nil {
		return vdb, fmt.Errorf("fail to complete add node operation, %w", runError)
	}
	return vdb, nil
}

// checkAddNodeRequirements returns an error if at least one of the nodes
// to add already exists in db.
func checkAddNodeRequirements(vdb *VCoordinationDatabase, hostsToAdd []string) error {
	// we don't want any of the new host to be part of the db.
	if nodes, _ := vdb.containNodes(hostsToAdd); len(nodes) != 0 {
		return fmt.Errorf("%s already exist in the database", strings.Join(nodes, ","))
	}

	return nil
}

// completeVDBSetting sets some VCoordinationDatabase fields we cannot get yet
// from the https endpoints. We set those fields from options.
func (o *VAddNodeOptions) completeVDBSetting(vdb *VCoordinationDatabase) error {
	vdb.DataPrefix = o.DataPrefix
	vdb.DepotPrefix = o.DepotPrefix

	hostNodeMap := makeVHostNodeMap()
	// TODO: we set the depot and data path from /nodes rather than manually
	// (VER-92725). This is useful for nmaDeleteDirectoriesOp.
	for h, vnode := range vdb.HostNodeMap {
		dataPath := vdb.genDataPath(vnode.Name)
		vnode.StorageLocations = append(vnode.StorageLocations, dataPath)
		if vdb.DepotPrefix != "" {
			vnode.DepotPath = vdb.genDepotPath(vnode.Name)
		}
		hostNodeMap[h] = vnode
	}
	vdb.HostNodeMap = hostNodeMap

	return nil
}

// trimNodesInCatalog removes failed node info from catalog
// which can be used to remove partially added nodes
func (vcc VClusterCommands) trimNodesInCatalog(vdb *VCoordinationDatabase,
	options *VAddNodeOptions) error {
	if len(options.ExpectedNodeNames) == 0 {
		vcc.Log.Info("ExpectedNodeNames is not set, skip trimming nodes", "ExpectedNodeNames", options.ExpectedNodeNames)
		return nil
	}

	// find out nodes to be trimmed
	// trimmed nodes are the ones in catalog but not expected
	expectedNodeNames := make(map[string]any)
	for _, nodeName := range options.ExpectedNodeNames {
		expectedNodeNames[nodeName] = struct{}{}
	}

	var aliveHosts []string
	var nodesToTrim []string
	nodeNamesInCatalog := make(map[string]any)
	for h, vnode := range vdb.HostNodeMap {
		nodeNamesInCatalog[vnode.Name] = struct{}{}
		if _, ok := expectedNodeNames[vnode.Name]; ok { // catalog node is expected
			aliveHosts = append(aliveHosts, h)
		} else { // catalog node is not expected, trim it
			// cannot trim UP nodes
			if vnode.State == util.NodeUpState {
				return fmt.Errorf("cannot trim the UP node %s (address %s)",
					vnode.Name, h)
			}
			nodesToTrim = append(nodesToTrim, vnode.Name)
		}
	}

	// sanity check: all provided node names should be found in catalog
	invalidNodeNames := util.MapKeyDiff(expectedNodeNames, nodeNamesInCatalog)
	if len(invalidNodeNames) > 0 {
		return fmt.Errorf("node names %v are not found in database %s",
			invalidNodeNames, vdb.Name)
	}

	vcc.Log.PrintInfo("Trim nodes %+v from catalog", nodesToTrim)

	// pick any up host as intiator
	initiator := aliveHosts[:1]

	var instructions []clusterOp

	// mark k-safety
	if len(aliveHosts) < ksafetyThreshold {
		httpsMarkDesignKSafeOp, err := makeHTTPSMarkDesignKSafeOp(initiator,
			options.usePassword, options.UserName, options.Password,
			ksafeValueZero)
		if err != nil {
			return err
		}
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	// remove down nodes from catalog
	for _, nodeName := range nodesToTrim {
		httpsDropNodeOp, err := makeHTTPSDropNodeOp(nodeName, initiator,
			options.usePassword, options.UserName, options.Password, vdb.IsEon)
		if err != nil {
			return err
		}
		instructions = append(instructions, &httpsDropNodeOp)
	}

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	err := clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.Error(err, "fail to trim nodes from catalog, %v")
		return err
	}

	// update vdb info
	vdb.HostNodeMap = util.FilterMapByKey(vdb.HostNodeMap, aliveHosts)
	vdb.HostList = aliveHosts

	return nil
}

// produceAddNodeInstructions will build a list of instructions to execute for
// the add node operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful add_node:
//   - Check NMA connectivity
//   - If we have subcluster in the input, check if the subcluster exists. If not, we stop.
//     If we do not have a subcluster in the input, fetch the current default subcluster name
//   - Check NMA versions
//   - Prepare directories
//   - Get network profiles
//   - Create the new node
//   - Reload spread
//   - Transfer config files to the new node
//   - Start the new node
//   - Poll node startup
//   - Create depot on the new node (Eon mode only)
//   - Sync catalog
//   - Rebalance shards on subcluster (Eon mode only)
func (vcc VClusterCommands) produceAddNodeInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions) ([]clusterOp, error) {
	var instructions []clusterOp
	initiatorHost := []string{options.Initiator}
	newHosts := options.NewHosts
	allExistingHosts := util.SliceDiff(vdb.HostList, options.NewHosts)
	username := options.UserName
	usePassword := options.usePassword
	password := options.Password

	nmaHealthOp := makeNMAHealthOp(vdb.HostList)
	instructions = append(instructions, &nmaHealthOp)

	if vdb.IsEon {
		httpsFindSubclusterOp, e := makeHTTPSFindSubclusterOp(
			allExistingHosts, usePassword, username, password, options.SCName,
			true /*ignore not found*/, AddNodeCmd)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsFindSubclusterOp)
	}

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOpWithVDB(true /*hosts need to have the same Vertica version*/, vdb)
	instructions = append(instructions, &nmaVerticaVersionOp)

	// this is a copy of the original HostNodeMap that only
	// contains the hosts to add.
	newHostNodeMap := vdb.copyHostNodeMap(options.NewHosts)
	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(newHostNodeMap,
		options.ForceRemoval /*force cleanup*/, false /*for db revive*/)
	if err != nil {
		return instructions, err
	}
	nmaNetworkProfileOp := makeNMANetworkProfileOp(vdb.HostList)
	httpsCreateNodeOp, err := makeHTTPSCreateNodeOp(newHosts, initiatorHost,
		usePassword, username, password, vdb, options.SCName)
	if err != nil {
		return instructions, err
	}
	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOpWithInitiator(initiatorHost, usePassword, username, password)
	if err != nil {
		return instructions, err
	}
	httpsRestartUpCommandOp, err := makeHTTPSStartUpCommandOp(usePassword, username, password, vdb)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&httpsCreateNodeOp,
		&httpsReloadSpreadOp,
		&httpsRestartUpCommandOp,
	)

	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		nil,
		vdb.HostList,
		vdb /*db configurations retrieved from a running db*/)

	nmaStartNewNodesOp := makeNMAStartNodeOpWithVDB(newHosts, options.StartUpConf, vdb)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(newHosts, usePassword, username, password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	return vcc.prepareAdditionalEonInstructions(vdb, options, instructions,
		username, usePassword, initiatorHost, newHosts)
}

func (vcc VClusterCommands) prepareAdditionalEonInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions,
	instructions []clusterOp,
	username string, usePassword bool,
	initiatorHost, newHosts []string) ([]clusterOp, error) {
	if vdb.UseDepot {
		httpsCreateNodesDepotOp, err := makeHTTPSCreateNodesDepotOp(vdb,
			newHosts, usePassword, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsCreateNodesDepotOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(initiatorHost, true, username, options.Password, AddNodeSyncCat)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
		if !*options.SkipRebalanceShards {
			httpsRBSCShardsOp, err := makeHTTPSRebalanceSubclusterShardsOp(
				initiatorHost, usePassword, username, options.Password, options.SCName)
			if err != nil {
				return instructions, err
			}
			instructions = append(instructions, &httpsRBSCShardsOp)
		}
	}

	return instructions, nil
}

// setInitiator sets the initiator as the first primary up node
func (o *VAddNodeOptions) setInitiator(primaryUpNodes []string) error {
	initiatorHost, err := getInitiatorHost(primaryUpNodes, []string{})
	if err != nil {
		return err
	}
	o.Initiator = initiatorHost
	return nil
}
