// Copyright (c) 2019, Sylabs Inc. All rights reserved.
// Copyright (c) 2025, NVIDIA Inc. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package slurm

import (
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/gvallee/go_exec/pkg/advexec"
	"github.com/gvallee/go_hpcjob/pkg/hpcjob"
)

const (
	// SlurmParitionKey is the key to use to retrieve the optinal parition id that
	// can be specified in the tool's configuration file.
	PartitionKey = "slurm_partition"

	// EnabledKey is the key used in the singularity-mpi.conf file to specify if Slurm shall be used
	EnabledKey = "enable_slurm"

	// ScriptCmdPrefix is the prefix to add to a script
	ScriptCmdPrefix = "#SBATCH"

	envVarNumNodes = "SLURM_JOB_NUM_NODES"

	envVarPartition = "SLURM_JOB_PARTITION"

	envVarListNodes = "SLURM_JOB_NODELIST"
)

func removeFromSlice(a []string, idx int) []string {
	return append(a[:idx], a[idx+1:]...)
}

func getJobStatus(jobID int) (hpcjob.Status, error) {
	var cmd advexec.Advcmd
	var err error
	cmd.BinPath, err = exec.LookPath("squeue")
	if err != nil {
		return hpcjob.StatusUnknown, err
	}
	cmd.CmdArgs = []string{"-j", strconv.Itoa(jobID), "--format=%t"}
	res := cmd.Run()
	if res.Err != nil {
		// if it fails it might mean the job is done
		res.Stderr = strings.TrimRight(res.Stderr, "\n")
		if strings.HasSuffix(res.Stderr, "Invalid job id specified") {
			return hpcjob.StatusDone, nil
		}
		return hpcjob.StatusUnknown, res.Err
	}

	lines := strings.Split(res.Stdout, "\n")
	// We do not care about the first lines, just the Slurm header and status that are not
	// relevant to us
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			lines = removeFromSlice(lines, i)
		}
	}
	rawStatus := strings.TrimRight(lines[len(lines)-1], "\n")
	switch rawStatus {
	case "R":
		return hpcjob.StatusRunning, nil
	case "PD":
		return hpcjob.StatusQueued, nil
	case "ST":
		// Try to get more details with a sacct command
		var sacctCmd advexec.Advcmd
		var err error
		sacctCmd.BinPath, err = exec.LookPath("sacct")
		if err != nil {
			return hpcjob.StatusUnknown, err
		}
		sacctCmd.CmdArgs = []string{"-j", strconv.Itoa(jobID), "--format=state"}
		resSacctCmd := sacctCmd.Run()
		if resSacctCmd.Err == nil {
			output := strings.Split(resSacctCmd.Stdout, "\n")
			if len(output) > 2 {
				if strings.Contains(output[2], "COMPLETED") {
					return hpcjob.StatusDone, nil
				}
			}
		}
		return hpcjob.StatusStop, nil
	}

	return hpcjob.StatusUnknown, nil
}

func GetNumJobs(partitionName string, user string) (int, error) {
	var cmd advexec.Advcmd
	var err error
	cmd.BinPath, err = exec.LookPath("squeue")
	if err != nil {
		return -1, err
	}
	cmd.CmdArgs = []string{"-p", partitionName, "-u", user}
	res := cmd.Run()
	if res.Err != nil {
		return -1, res.Err
	}

	lines := strings.Split(res.Stdout, "\n")
	numJobs := 0
	for _, line := range lines {
		if strings.Contains(line, "JOBID") || line == "" {
			continue
		}
		numJobs++
	}

	return numJobs, nil
}

func JobStatus(jobIDs []int) ([]hpcjob.Status, error) {
	var s []hpcjob.Status

	for _, jobID := range jobIDs {
		jobStatus, err := getJobStatus(jobID)
		if err != nil {
			return nil, err
		}
		s = append(s, jobStatus)
	}

	return s, nil
}

func GetNumNodes() (int, error) {
	valStr := os.Getenv(envVarNumNodes)
	return strconv.Atoi(valStr)
}

func GetPartition() string {
	return os.Getenv(envVarPartition)
}

func GetListNode() string {
	return os.Getenv(envVarListNodes)
}
