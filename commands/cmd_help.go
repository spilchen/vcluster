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
	"fmt"
	"os"

	"vertica.com/vcluster/vclusterops/vlog"
)

/* CmdHelp
 *
 * A command providing top-level help on
 * various topics. PrintUsage() will print
 * the requested help.
 *
 * Implements ClusterCommand interface
 */
type CmdHelp struct {
	argv   []string
	parser *flag.FlagSet
	topic  *string
}

func MakeCmdHelp() CmdHelp {
	newCmd := CmdHelp{}
	newCmd.parser = flag.NewFlagSet("help", flag.ExitOnError)
	newCmd.topic = newCmd.parser.String("topic", "", "The topic for more help")
	return newCmd
}

func (c CmdHelp) CommandType() string {
	return "help"
}

func (c *CmdHelp) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv

	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	return c.validateParse()
}

func (c *CmdHelp) validateParse() error {
	vlog.LogInfoln("Called validateParse()")
	return nil
}

func (c *CmdHelp) Analyze() error {
	return nil
}

func (c *CmdHelp) Run() error {
	return nil
}

func (c *CmdHelp) PrintUsage() {
	fmt.Fprintf(os.Stderr,
		"vcluster %s\nExample: vcluster %s --topic create_db\n",
		c.CommandType(),
		c.CommandType())
	c.parser.PrintDefaults()
}
