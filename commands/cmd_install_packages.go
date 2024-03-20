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
	"encoding/json"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
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

func makeCmdInstallPackages() *cobra.Command {
	// CmdInstallPackages
	newCmd := &CmdInstallPackages{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VInstallPackagesOptionsFactory()
	newCmd.installPkgOpts = &opt

	cmd := OldMakeBasicCobraCmd(
		newCmd,
		installPkgSubCmd,
		"Install default package(s) in database",
		`This subcommand installs default packages in the database.

The default packages are those under /opt/vertica/packages where Autoinstall is marked true.
Per package installation status will be returned.

Examples:
  # Install default packages using user input.
  vcluster install_packages --db-name test_db --hosts vnode1,vnode2,vnode3

  # Force (re)install default packages using config file.
  vcluster install_packages --db-name test_db --force-reinstall --config /opt/vertica/config/vertica_cluster.yaml
`,
	)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{dbNameFlag, configFlag, hostsFlag, passwordFlag,
		outputFileFlag})

	// local flags
	newCmd.setLocalFlags(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdInstallPackages) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.installPkgOpts.ForceReinstall,
		"force-reinstall",
		false,
		"Install the packages, even if they are already installed.",
	)
}

func (c *CmdInstallPackages) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.OldResetUserInputOptions()

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdInstallPackages) validateParse() error {
	err := c.getCertFilesFromCertPaths(&c.installPkgOpts.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.installPkgOpts.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.installPkgOpts.DatabaseOptions)
}

func (c *CmdInstallPackages) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdInstallPackages) Run(vcc vclusterops.ClusterCommands) error {
	options := c.installPkgOpts

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	var status *vclusterops.InstallPackageStatus
	status, err = vcc.VInstallPackages(options)
	if err != nil {
		vcc.LogError(err, "failed to install the packages")
		return err
	}

	var bytes []byte
	bytes, err = json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}

	c.writeCmdOutputToFile(globals.file, bytes, vcc.GetLog())
	vcc.LogInfo("Installed the packages: ", "packages", string(bytes))

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdInstallPackages
func (c *CmdInstallPackages) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.installPkgOpts.DatabaseOptions = *opt
}
