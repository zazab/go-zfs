package zfs

import (
	"errors"
	"io"
	"strings"

	"github.com/theairkit/runcmd"
)

// See Zfs.NewSnapshot
func NewSnapshot(snapshotPath string) Snapshot {
	return std.NewSnapshot(snapshotPath)
}

// Return Snapshot wrapper without any checks and actualy creation
func (z Zfs) NewSnapshot(snap string) Snapshot {
	buf := strings.Split(snap, "@")
	path := buf[0]
	name := buf[1]
	return Snapshot{zfsEntryBase{z, snap}, NewFs(path), name}
}

type Snapshot struct {
	zfsEntryBase
	Fs   Fs
	Name string
}

func (s Snapshot) Clone(targetPath string) (Fs, error) {
	if s.GetPool() != NewFs(targetPath).GetPool() {
		return Fs{}, PoolError
	}
	c, err := s.runner.Command("zfs clone " + s.Path + " " + targetPath)
	if err != nil {
		return Fs{}, errors.New("error creating clone: " + err.Error())
	}
	_, err = c.Run()
	if err != nil {
		return Fs{}, errors.New("error creating clone: " + err.Error())
	}

	return Fs{zfsEntryBase{s.runner, targetPath}}, nil
}

func (f Fs) Snapshot(name string) (Snapshot, error) {
	snapshotPath := f.Path + "@" + name
	c, err := f.runner.Command("zfs snapshot " + snapshotPath)
	if err != nil {
		return Snapshot{},
			errors.New("error creating snapshot: " + err.Error())
	}
	_, err = c.Run()
	if err != nil {
		return Snapshot{}, parseError(err)
	}

	snap := Snapshot{zfsEntryBase{f.runner, snapshotPath}, f, name}
	return snap, nil
}

func (f Fs) ListSnapshots() ([]Snapshot, error) {
	c, err := f.runner.Command("zfs list -Hr -o name -t snapshot " + f.Path)
	if err != nil {
		return []Snapshot{},
			errors.New("error getting snapshot list: " + err.Error())
	}
	out, err := c.Run()
	if err != nil {
		return []Snapshot{}, parseError(err)
	}

	snapshots := []Snapshot{}
	for _, snap := range out {
		snapName := strings.Split(snap, "@")[1]
		snapshots = append(snapshots, Snapshot{
			zfsEntryBase{f.runner, snap},
			f,
			snapName,
		})
	}
	return snapshots, nil
}

func notExits(e zfsEntry) error {
	return errors.New("cannot open '" + e.getPath() + "': dataset does not exist")
}

func (s Snapshot) Send(to zfsEntry) error {
	rc, err := to.Receive()
	if err != nil {
		return err
	}

	return s.SendStream(rc)
}

func (s Snapshot) SendInc(base Snapshot, to zfsEntry) error {
	rc, err := to.Receive()
	if err != nil {
		return err
	}

	return s.SendIncStream(base, rc)
}

func (s Snapshot) SendStream(dest runcmd.CmdWorker) error {
	if ok, _ := s.Exists(); !ok {
		return notExits(s)
	}

	c, err := s.runner.Command("zfs send " + s.Path)
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}

	_, err = io.Copy(dest.StdinPipe(), c.StdoutPipe())
	if err != nil {
		return err
	}

	err = dest.Wait()
	if err != nil {
		return parseError(err)
	}

	return parseError(c.Wait())
}

func (s Snapshot) SendIncStream(base Snapshot, dest runcmd.CmdWorker) error {
	if ok, _ := s.Exists(); !ok {
		return notExits(s)
	}
	if ok, _ := base.Exists(); !ok {
		return notExits(base)
	}

	c, err := s.runner.Command("zfs send -i " + base.Path + " " + s.Path)
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}

	_, err = io.Copy(dest.StdinPipe(), c.StdoutPipe())
	if err != nil {
		return err
	}

	err = dest.Wait()
	if err != nil {
		return parseError(err)
	}

	return parseError(c.Wait())
}
