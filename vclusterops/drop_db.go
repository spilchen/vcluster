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
)

// VDropDatabaseOptions adds to VCreateDatabaseOptions the option to force delete directories.
type VDropDatabaseOptions struct {
	VCreateDatabaseOptions
	ForceDelete *bool // whether force delete directories
}

func VDropDatabaseOptionsFactory() VDropDatabaseOptions {
	opt := VDropDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

// AnalyzeOptions verifies the host options for the VDropDatabaseOptions struct and
// returns any error encountered.
func (options *VDropDatabaseOptions) AnalyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.Hosts = hostAddresses
	return nil
}

func (options *VDropDatabaseOptions) validateAnalyzeOptions() error {
	if *options.DBName == "" {
		return fmt.Errorf("database name must be provided")
	}
	return nil
}

func (vcc *VClusterCommands) VDropDatabase(options *VDropDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions()
	if err != nil {
		return err
	}

	// Analyze to produce vdb info for drop db use
	vdb := makeVCoordinationDatabase()

	// TODO: this currently requires a config file to exist. We should allow
	// drop to proceed with just options provided and no config file.

	// load vdb info from the YAML config file.
	clusterConfig, err := ReadConfig(options.ConfigPath, vcc.Log)
	if err != nil {
		return err
	}
	err = vdb.setFromClusterConfig(*options.DBName, &clusterConfig)
	if err != nil {
		return err
	}

	// produce drop_db instructions
	instructions, err := vcc.produceDropDBInstructions(&vdb, options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to drop database: %w", runError)
	}

	// if the database is successfully dropped, the database will be removed from the config file
	// if failed to remove it, we will ask users to manually do it
	err = clusterConfig.removeDatabaseFromConfigFile(vdb.Name, options.ConfigPath, vcc.Log)
	if err != nil {
		vcc.Log.PrintWarning("Fail to remove the database information from config file, "+
			"please manually clean up under directory %s. Details: %v", options.ConfigPath, err)
	}

	return nil
}

// produceDropDBInstructions will build a list of instructions to execute for
// the drop db operation
//
// The generated instructions will later perform the following operations necessary
// for a successful drop_db:
//   - Check NMA connectivity
//   - Check to see if any dbs running
//   - Delete directories
func (vcc *VClusterCommands) produceDropDBInstructions(vdb *VCoordinationDatabase, options *VDropDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := vdb.HostList
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	nmaHealthOp := makeNMAHealthOp(hosts)

	// when checking the running database,
	// drop_db has the same checking items with create_db
	checkDBRunningOp, err := makeHTTPSCheckRunningDBOp(hosts, usePassword,
		*options.UserName, options.Password, CreateDB)
	if err != nil {
		return instructions, err
	}

	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(vdb, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&checkDBRunningOp,
		&nmaDeleteDirectoriesOp,
	)

	return instructions, nil
}
