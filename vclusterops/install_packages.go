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

type VInstallPackagesOptions struct {
	/* part 1: basic db info */
	DatabaseOptions

	// If true, the packages will be reinstalled even if they are already installed.
	ForceReinstall *bool
}

type vInstallPackagesInfo struct {
	dbName   string
	hosts    []string
	userName string
	password *string
}

func VInstallPackagesOptionsFactory() VInstallPackagesOptions {
	opt := VInstallPackagesOptions{
		ForceReinstall: new(bool),
	}
	opt.DatabaseOptions.setDefaultValues()
	return opt
}

// resolve hostnames to be IPs
func (options *VInstallPackagesOptions) analyzeOptions() (err error) {
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VInstallPackagesOptions) validateAnalyzeOptions(log vlog.Printer) error {
	if err := options.validateBaseOptions("install_packages", log); err != nil {
		return err
	}
	return options.analyzeOptions()
}

func (vcc *VClusterCommands) VInstallPackages(options *VInstallPackagesOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return err
	}

	installPkgInfo := new(vInstallPackagesInfo)
	installPkgInfo.userName = *options.UserName
	installPkgInfo.password = options.Password
	installPkgInfo.dbName, installPkgInfo.hosts, err = options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	instructions, err := vcc.produceInstallPackagesInstructions(installPkgInfo, options)
	if err != nil {
		return fmt.Errorf("fail to production instructions: %w", err)
	}

	// Create a VClusterOpEngine. No need for certs since this operation doesn't
	// talk to the NMA.
	clusterOpEngine := makeClusterOpEngine(instructions, &httpsCerts{})

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to install packages: %w", runError)
	}

	return nil
}

// produceInstallPackagesInstructions will build a list of instructions to execute for
// the install packages operation.
//
// The generated instructions will later perform the following operations necessary
// to install packages:
//   - Get up nodes through https call
//   - Install packages using one of the up nodes
func (vcc *VClusterCommands) produceInstallPackagesInstructions(info *vInstallPackagesInfo,
	opts *VInstallPackagesOptions,
) ([]clusterOp, error) {
	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if info.password != nil {
		usePassword = true
		err := opts.validateUserName(vcc.Log)
		if err != nil {
			return nil, err
		}
	}

	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(vcc.Log, info.dbName, info.hosts,
		usePassword, *opts.UserName, info.password, InstallPackageCmd)
	if err != nil {
		return nil, err
	}

	var noHosts = []string{} // We pass in no hosts so that this op picks an up node from the previous call.
	installOp, err := makeHTTPSInstallPackagesOp(vcc.Log, noHosts, usePassword, *opts.UserName, info.password, *opts.ForceReinstall)
	if err != nil {
		return nil, err
	}

	instructions := []clusterOp{
		&httpsGetUpNodesOp,
		&installOp,
	}

	return instructions, nil
}
