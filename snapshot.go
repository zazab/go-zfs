package zfs

import (
	"errors"
	"strings"
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
	return Snapshot{zfsEntry{z, snap}, NewFs(path), name}
}

type Snapshot struct {
	zfsEntry
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

	return Fs{zfsEntry{s.runner, targetPath}}, nil
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
		return Snapshot{},
			errors.New("error creating snapshot: " + err.Error())
	}

	snap := Snapshot{zfsEntry{f.runner, snapshotPath}, f, name}
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
		return []Snapshot{},
			errors.New("error getting snapshot list: " + err.Error())
	}

	snapshots := []Snapshot{}
	for _, snap := range out {
		snapName := strings.Split(snap, "@")[1]
		snapshots = append(snapshots, Snapshot{
			zfsEntry{f.runner, snap},
			f,
			snapName,
		})
	}
	return snapshots, nil
}
