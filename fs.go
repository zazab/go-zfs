package zfs

import (
	"errors"
	"strconv"
	"strings"

	"github.com/theairkit/runcmd"
)

type zfsEntry struct {
	runner runcmd.Runner
	Path   string
}

func (z zfsEntry) GetProperty(prop string) (string, error) {
	c, err := z.runner.Command("zfs get -H -o value " + prop + " " + z.Path)
	if err != nil {
		return "", errors.New("error getting property: " + err.Error())
	}
	out, err := c.Run()
	if err != nil {
		return "", errors.New("error getting property: " + err.Error())
	}
	return out[0], nil

}

func (z zfsEntry) SetProperty(prop, value string) error {
	c, err := z.runner.Command("zfs set " + prop + "=" + value + " " + z.Path)
	if err != nil {
		return err
	}
	if _, err = c.Run(); err != nil {
		return err
	}
	out, err := z.GetProperty(prop)
	if err != nil {
		return err
	}
	if out != value {
		return errors.New("cannot set property: " + prop)
	}
	return nil
}

func (z zfsEntry) Destroy(recursive bool) error {
	cmd := "zfs destroy "
	if recursive {
		cmd += "-R "
	}
	c, err := z.runner.Command(cmd + z.Path)
	if err != nil {
		return err
	}
	_, err = c.Run()
	return err
}

func (z zfsEntry) GetPool() string {
	buf := strings.SplitN(z.Path, "/", 2)
	return buf[0]
}

func (z zfsEntry) GetLastPath() string {
	buf := strings.Split(z.Path, "/")
	return buf[len(buf)-1]
}

type Fs struct {
	zfsEntry
}

// See Zfs.CreateFs
func CreateFs(zfsPath string) (Fs, error) {
	return std.CreateFs(zfsPath)
}

// Actually creates filesystem
func (z Zfs) CreateFs(zfsPath string) (Fs, error) {
	c, err := z.runner.Command("zfs create " + zfsPath)
	if err != nil {
		return z.NewFs(zfsPath), errors.New("error creating fs: " + err.Error())
	}
	_, err = c.Run()
	if err != nil {
		return z.NewFs(zfsPath), errors.New("error creating fs: " + err.Error())
	}

	return z.NewFs(zfsPath), nil
}

// See Zfs.NewFs
func NewFs(zfsPath string) Fs {
	return std.NewFs(zfsPath)
}

// Return Fs wrapper without any checks and actualy creation
func (z Zfs) NewFs(zfsPath string) Fs {
	return Fs{zfsEntry{z.runner, zfsPath}}
}

// See Zfs.ListFs
func ListFs(path string) ([]Fs, error) {
	return std.ListFs(path)
}

// Return list of all found filesystems
func (z Zfs) ListFs(path string) ([]Fs, error) {
	c, err := z.runner.Command("zfs list -Hr -o name " + path)
	if err != nil {
		return []Fs{}, errors.New("error listing fs: " + err.Error())
	}

	out, err := c.Run()
	if err != nil {
		return []Fs{}, errors.New("error listing fs: " + err.Error())
	}

	filesystems := []Fs{}
	for _, fs := range out {
		filesystems = append(filesystems, z.NewFs(fs))
	}

	return filesystems, nil
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

func (f Fs) Promote() error {
	c, err := f.runner.Command("zfs promote " + f.Path)
	if err != nil {
		return errors.New("error promoting fs: " + err.Error())
	}
	_, err = c.Run()
	if err != nil {
		return errors.New("error promoting fs: " + err.Error())
	}

	return nil
}

func (f Fs) GetSize() (int, error) {
	c, err := f.runner.Command("zfs get -Hp -o value usedbydataset " + f.Path)
	if err != nil {
		return 0, errors.New("error getting fs size: " + err.Error())
	}
	out, err := c.Run()
	if err != nil {
		return 0, errors.New("error getting fs size: " + err.Error())
	}

	return strconv.Atoi(out[0])
}
