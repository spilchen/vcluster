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

package commands

import (
	"flag"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdInstallPackages
 *
 * Parses arguments for VInstallPackagesOptions to pass down to
 * VInstallPackages.
 *
 * Implements ClusterCommand interface
 */

type CmdInstallPackages struct {
	CmdBase
	installPkgOpts *vclusterops.VInstallPackagesOptions
}

func makeCmdInstallPackages() *CmdInstallPackages {
	newCmd := &CmdInstallPackages{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("install_packages", flag.ExitOnError)
	installPkgOpts := vclusterops.VInstallPackagesOptionsFactory()

	// required flags
	installPkgOpts.DBName = newCmd.parser.String("db-name", "", "The name of the database to install packages in")

	// optional flags
	installPkgOpts.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts in database."))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Used to specify the hosts are IPv6 hosts"))
	installPkgOpts.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	installPkgOpts.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	installPkgOpts.ForceReinstall = newCmd.parser.Bool("force-reinstall", false,
		util.GetOptionalFlagMsg("Install the packages, even if they are already installed."))

	newCmd.installPkgOpts = &installPkgOpts

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "install_packages")
	}

	return newCmd
}

func (c *CmdInstallPackages) CommandType() string {
	return "install_packages"
}

func (c *CmdInstallPackages) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.installPkgOpts.Password = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.installPkgOpts.ConfigDirectory = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdInstallPackages) validateParse() error {
	return c.ValidateParseBaseOptions(&c.installPkgOpts.DatabaseOptions)
}

func (c *CmdInstallPackages) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdInstallPackages) Run(vcc vclusterops.VClusterCommands) error {
	options := c.installPkgOpts

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VInstallPackages(options)
	if err != nil {
		vcc.Log.Error(err, "failed to install the packages")
		return err
	}

	// SPILLY - dump out the packages that were installed?
	vcc.Log.PrintInfo("Installed the packages")
	return nil
}
