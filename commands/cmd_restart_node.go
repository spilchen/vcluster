package commands

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const Secs = "seconds"

/* CmdRestartNode
 *
 * Implements ClusterCommand interface
 */
type CmdRestartNodes struct {
	CmdBase
	restartNodesOptions *vclusterops.VStartNodesOptions

	// Comma-separated list of vnode=host
	vnodeListStr *string
}

func makeCmdRestartNodes() *CmdRestartNodes {
	// CmdRestartNodes
	newCmd := &CmdRestartNodes{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("restart_node", flag.ExitOnError)
	restartNodesOptions := vclusterops.VStartNodesOptionsFactory()

	// require flags
	restartNodesOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to restart nodes")
	newCmd.vnodeListStr = newCmd.parser.String("restart", "",
		"Comma-separated list of NODENAME=REIPHOST pairs part of the database nodes that need to be restarted")

	// optional flags
	restartNodesOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated hosts that participate in the database"+
		NotTrust+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "restart nodes with IPv6 hosts")

	restartNodesOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user input instead of reading the options from "+vclusterops.ConfigFileName))
	restartNodesOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg(DirWhr+vclusterops.ConfigFileName+Located))
	restartNodesOptions.StatePollingTimeout = *newCmd.parser.Int("timeout", util.DefaultTimeoutSeconds,
		util.GetOptionalFlagMsg(setTimeOutMsg+
			strconv.Itoa(util.DefaultTimeoutSeconds)+Secs))

	newCmd.restartNodesOptions = &restartNodesOptions
	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "restart_node")
	}
	return newCmd
}

func (c *CmdRestartNodes) CommandType() string {
	return "restart_node"
}

func (c *CmdRestartNodes) Parse(inputArgv []string, logger vlog.Printer) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.restartNodesOptions.ConfigDirectory = nil
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdRestartNodes) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.restartNodesOptions.ParseNodesList(*c.vnodeListStr)
	if err != nil {
		return err
	}
	return c.ValidateParseBaseOptions(&c.restartNodesOptions.DatabaseOptions)
}

func (c *CmdRestartNodes) Analyze(logger vlog.Printer) error {
	// Analyze() is needed to fulfill an interface
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdRestartNodes) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.restartNodesOptions

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	// this is the instruction that will be used by both CLI and operator
	err = vcc.VStartNodes(options)
	if err != nil {
		return err
	}

	var hostToRestart []string
	for _, ip := range options.Nodes {
		hostToRestart = append(hostToRestart, ip)
	}
	vcc.Log.PrintInfo("Successfully restart hosts %s of the database %s", hostToRestart, *options.DBName)

	return nil
}
