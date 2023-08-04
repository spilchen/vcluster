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

package vlog

import (
	"fmt"
	"log"
	"os"
	"sync"

	"runtime/debug"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

const (
	DefaultLogPath = "/opt/vertica/log/vcluster.log"
	LogPermission  = 0644

	InfoLog    = "[INFO] "
	WarningLog = "[WARNING] "
	ErrorLog   = "[ERROR] "
	DebugLog   = "[DEBUG] "
)

type Vlogger struct {
	LogPath string
	Log     logr.Logger // Logging API to use for all logging calls
}

var (
	logInstance Vlogger
	once        sync.Once
)

// return a singleton instance of the GlobalLogger
func GetGlobalLogger() *Vlogger {
	/* if once.Do(f) is called multiple times,
	 * only the first call will invoke f,
	 * even if f has a different value in each invocation.
	 * Reference: https://pkg.go.dev/sync#Once
	 */
	once.Do(func() {
		logInstance = makeGlobalLogger()
	})

	return &logInstance
}

func makeGlobalLogger() Vlogger {
	newGlobalLogger := Vlogger{}
	return newGlobalLogger
}

func ParseLogPathArg(argInput []string, defaultPath string) string {
	logger := GetGlobalLogger()
	return logger.parseLogPathArg(argInput, defaultPath)
}
func (logger *Vlogger) parseLogPathArg(argInput []string, defaultPath string) string {
	checkLogDir := true
	for idx, arg := range argInput {
		if arg == "--log-path" {
			logger.LogPath = argInput[idx+1]
			checkLogDir = false
		}
	}
	if checkLogDir {
		logger.LogPath = defaultPath
	}
	return logger.LogPath
}

func SetupOrDie(logFile string) {
	logger := GetGlobalLogger()
	logger.setupOrDie(logFile)
}

// setupOrDie will setup the logging for vcluster CLI. On exit, logger.Log will
// be set.
func (logger *Vlogger) setupOrDie(logFile string) {
	// The vcluster library uses logr as the logging API. We use Uber's zap
	// package to implement the logging API.
	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		// Sampling is enabled at 100:100, meaning that after the first 100 log
		// entries with the same level and message in the same second, it will
		// log every 100th entry with the same level and message in the same second.
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	// If no log file is given, we just log to standard output
	if logFile != "" {
		cfg.OutputPaths = []string{logFile}
	}
	var err error
	zapLg, err := cfg.Build()
	if err != nil {
		logger.logFatal(err)
	}
	logger.Log = zapr.NewLogger(zapLg)
	logger.Log.Info("Successfully started logger", "logFile", logFile)
}

func LogStartupMessage() error {
	logger := GetGlobalLogger()
	return logger.logStartupMessage()
}

func (logger *Vlogger) logStartupMessage() error {
	// all INFO level log
	logger.logInfo("New log for process %d", os.Getpid())
	logger.logInfo("Called with args %s", os.Args)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	logger.logInfo("Hostname %s, User id %d", hostname, os.Getuid())
	return nil
}

func LogFatal(err error) {
	logger := GetGlobalLogger()
	logger.logFatal(err)
}

func (logger *Vlogger) logFatal(err error) {
	if err == nil {
		return
	}
	stackBytes := debug.Stack()
	logger.logInfo("Fatal error occurred. Backtrace:\n%s\n", string(stackBytes))
	log.Fatal(err)
}

func LogInfoln(info string) {
	logger := GetGlobalLogger()
	logger.logInfoln(info)
}

// basic log functions starts here: log plain string
// following log.Println naming convention
func (logger *Vlogger) logInfoln(info string) {
	log.Println(InfoLog + info)
}

func LogWarningln(info string) {
	logger := GetGlobalLogger()
	logger.logWarningln(info)
}

// log Warning
func (logger *Vlogger) logWarningln(info string) {
	log.Println(WarningLog + info)
}

func LogErrorln(info string) {
	logger := GetGlobalLogger()
	logger.logErrorln(info)
}

// log error
func (logger *Vlogger) logErrorln(info string) {
	log.Println(ErrorLog + info)
}

func LogInfo(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logInfo(info, v...)
}

// log info with formatting
func (logger *Vlogger) logInfo(info string, v ...any) {
	log.Printf(InfoLog+info, v...)
}

func LogWarning(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logWarning(info, v...)
}
func (logger *Vlogger) logWarning(info string, v ...any) {
	log.Printf(WarningLog+info, v...)
}

func LogError(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logError(info, v...)
}
func (logger *Vlogger) logError(info string, v ...any) {
	log.Printf(ErrorLog+info, v...)
}

func LogDebug(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logDebug(info, v...)
}
func (logger *Vlogger) logDebug(info string, v ...any) {
	log.Printf(DebugLog+info, v...)
}

// output to both log and console
// another possible way is to use io.MultiWriter, but that needs set a different output:
// mw := io.MultiWriter(os.Stdout, logFile)
// log.SetOutput(mw)
// so to avoid setting output back and forth, we just do a log and fmt
// log and print msg of the format "levelPrefix msg"
// e.g., [Info] this is a sample log info
func (logger *Vlogger) logPrintInternal(msg string) {
	logger.Log.Info(msg)
	fmt.Println(msg)
}

func LogPrintInfo(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintInfo(msg, v...)
}
func (logger *Vlogger) logPrintInfo(msg string, v ...any) {
	completeMsg := fmt.Sprintf(InfoLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintError(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintError(msg, v...)
}
func (logger *Vlogger) logPrintError(msg string, v ...any) {
	completeMsg := fmt.Errorf(ErrorLog+msg, v...)
	logger.logPrintInternal(completeMsg.Error())
}

func LogPrintDebug(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintDebug(msg, v...)
}
func (logger *Vlogger) logPrintDebug(msg string, v ...any) {
	completeMsg := fmt.Sprintf(DebugLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintWarning(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintWarning(msg, v...)
}
func (logger *Vlogger) logPrintWarning(msg string, v ...any) {
	completeMsg := fmt.Sprintf(WarningLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintInfoln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintInfoln(msg)
}
func (logger *Vlogger) logPrintInfoln(msg string) {
	logger.logPrintInternal(InfoLog + msg)
}

func LogPrintWarningln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintWarningln(msg)
}
func (logger *Vlogger) logPrintWarningln(msg string) {
	logger.logPrintInternal(WarningLog + msg)
}

func LogPrintErrorln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintErrorln(msg)
}
func (logger *Vlogger) logPrintErrorln(msg string) {
	logger.logPrintInternal(ErrorLog + msg)
}

func LogArgParse(inputArgv *[]string) {
	logger := GetGlobalLogger()
	logger.logArgParse(inputArgv)
}

// log functions for specific cases
func (logger *Vlogger) logArgParse(inputArgv *[]string) {
	inputArgMsg := fmt.Sprintf("Called method Parse with args: %q.", *inputArgv)
	logger.logInfoln(inputArgMsg)
}
