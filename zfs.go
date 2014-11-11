package zfs

import "github.com/theairkit/runcmd"

type Zfs struct {
	runner ZfsRunner
}

type ZfsRunner struct {
	runcmd.Runner
	sudo bool
}

func (z ZfsRunner) Command(cmd string) (runcmd.CmdWorker, error) {
	if z.sudo {
		cmd = "sudo " + cmd
	}
	return z.Runner.Command(cmd)
}

var std, _ = NewZfsLocal(false)

func NewZfsLocal(sudo bool) (Zfs, error) {
	runner, err := runcmd.NewLocalRunner()
	return Zfs{ZfsRunner{runner, sudo}}, err
}

func NewZfs(runner runcmd.Runner, sudo bool) Zfs {
	return Zfs{ZfsRunner{runner, sudo}}
}

func SetStdSudo(sudo bool) error {
	var err error
	std, err = NewZfsLocal(sudo)
	return err
}
