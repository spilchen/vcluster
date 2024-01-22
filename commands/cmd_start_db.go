package commands

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStartDB
 *
 * Implements ClusterCommand interface
 */
type CmdStartDB struct {
	CmdBase
	startDBOptions *vclusterops.VStartDatabaseOptions

	Force               *bool   // force cleanup to start the database
	AllowFallbackKeygen *bool   // Generate spread encryption key from Vertica. Use under support guidance only
	IgnoreClusterLease  *bool   // ignore the cluster lease in communal storage
	Unsafe              *bool   // Start database unsafely, skipping recovery.
	Fast                *bool   // Attempt fast startup database
	configurationParams *string // raw input from user, need further processing
}

const setTimeOutMsg = "Set a timeout (in seconds) for polling node state operation, default timeout is "

func makeCmdStartDB() *CmdStartDB {
	// CmdStartDB
	newCmd := &CmdStartDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("start_db", flag.ExitOnError)
	startDBOptions := vclusterops.VStartDatabaseOptionsFactory()

	// require flags
	startDBOptions.DBName = newCmd.parser.String("db-name", "", util.GetOptionalFlagMsg("The name of the database to be started."+
		NotTrust+vclusterops.ConfigFileName))

	// optional flags
	startDBOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	startDBOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", "The catalog path of the database")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg(commaSeparatedLog+NotTrust+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "start database with with IPv6 hosts")

	startDBOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg(flagMsg+vclusterops.ConfigFileName))
	startDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg(DirWhr+vclusterops.ConfigFileName+Located))
	startDBOptions.StatePollingTimeout = newCmd.parser.Int("timeout", util.DefaultTimeoutSeconds,
		util.GetOptionalFlagMsg(setTimeOutMsg+
			strconv.Itoa(util.DefaultTimeoutSeconds)+Secs))
	// eon flags
	newCmd.isEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("Indicate if the database is an Eon database."+
		NotTrust+vclusterops.ConfigFileName))
	startDBOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))
	newCmd.configurationParams = newCmd.parser.String("config-param", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for configuration parameters"))

	// hidden options
	// TODO: the following options will be processed later
	newCmd.Unsafe = newCmd.parser.Bool("unsafe", false, util.SuppressHelp)
	newCmd.Force = newCmd.parser.Bool("force", false, util.SuppressHelp)
	newCmd.AllowFallbackKeygen = newCmd.parser.Bool("allow_fallback_keygen", false, util.SuppressHelp)
	newCmd.IgnoreClusterLease = newCmd.parser.Bool("ignore_cluster_lease", false, util.SuppressHelp)
	newCmd.Fast = newCmd.parser.Bool("fast", false, util.SuppressHelp)
	startDBOptions.TrimHostList = newCmd.parser.Bool("trim-hosts", false, util.SuppressHelp)

	newCmd.startDBOptions = &startDBOptions
	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "start_db")
	}
	return newCmd
}

func (c *CmdStartDB) CommandType() string {
	return "start_db"
}

func (c *CmdStartDB) Parse(inputArgv []string, logger vlog.Printer) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.startDBOptions.ConfigDirectory = nil
	}

	return c.validateParse(logger)
}

func (c *CmdStartDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", c.CommandType())

	// check the format of configuration params string, and parse it into configParams
	configurationParams, err := util.ParseConfigParams(*c.configurationParams)
	if err != nil {
		return err
	}
	if configurationParams != nil {
		c.startDBOptions.ConfigurationParameters = configurationParams
	}

	return c.ValidateParseBaseOptions(&c.startDBOptions.DatabaseOptions)
}

func (c *CmdStartDB) Analyze(logger vlog.Printer) error {
	// Analyze() is needed to fulfill an interface
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdStartDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.startDBOptions

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VStartDatabase(options)
	if err != nil {
		vcc.Log.Error(err, "failed to start the database")
		return err
	}

	vcc.Log.PrintInfo("Successfully start the database %s\n", *options.DBName)
	return nil
}
