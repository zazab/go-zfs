package zfs

import "github.com/theairkit/runcmd"

type Zfs struct {
	*ZfsRunner
}

type ZfsRunner struct {
	runcmd.Runner
	sudo bool
}

var std = mustCreateRunner(NewZfsLocal(false))

func (z ZfsRunner) Command(name string, args ...string) runcmd.CmdWorker {
	if z.sudo {
		args = append([]string{name}, args...)
		name = "sudo"
	}

	return z.Runner.Command(name, args...)
}

func NewZfsLocal(sudo bool) (Zfs, error) {
	runner, err := runcmd.NewLocalRunner()
	return Zfs{&ZfsRunner{runner, sudo}}, err
}

func NewZfs(runner runcmd.Runner, sudo bool) Zfs {
	return Zfs{&ZfsRunner{runner, sudo}}
}

func SetStdSudo(sudo bool) {
	std.sudo = sudo
}

func mustCreateRunner(operator Zfs, err error) *Zfs {
	if err != nil {
		panic(err)
	}

	return &operator
}
