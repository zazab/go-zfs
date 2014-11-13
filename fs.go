package zfs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/theairkit/runcmd"
)

type zfsEntry interface {
	GetProperty(string) (string, error)
	SetProperty(string, string) error
	GetPool() string
	GetLastPath() string
	Destroy(bool) error
	Exists() (bool, error)
	Receive() (runcmd.CmdWorker, error)
	getPath() string
}

type zfsEntryBase struct {
	runner Zfs
	Path   string
}

func (z zfsEntryBase) GetProperty(prop string) (string, error) {
	c, err := z.runner.Command("zfs get -Hp -o value " + prop + " " + z.Path)
	if err != nil {
		return "", err
	}
	out, err := c.Run()
	if err != nil {
		return "", parseError(err)
	}
	return out[0], nil

}

func (z zfsEntryBase) getPath() string {
	return z.Path
}

func (z zfsEntryBase) SetProperty(prop, value string) error {
	c, err := z.runner.Command("zfs set " + prop + "=" + value + " " + z.Path)
	if err != nil {
		return err
	}
	if _, err = c.Run(); err != nil {
		return parseError(err)
	}
	out, err := z.GetProperty(prop)
	if err != nil {
		return err
	}
	if out != value {
		return errors.New("property " + prop + " not set")
	}
	return nil
}

func (z zfsEntryBase) Destroy(recursive bool) error {
	cmd := "zfs destroy "
	if recursive {
		cmd += "-R "
	}
	c, err := z.runner.Command(cmd + z.Path)
	if err != nil {
		return err
	}
	_, err = c.Run()
	return parseError(err)
}

func (z zfsEntryBase) Exists() (bool, error) {
	c, err := z.runner.Command("zfs list -H -o name " + z.Path)
	if err != nil {
		return false, errors.New("error initializing existance check: " + err.Error())
	}

	_, err = c.Run()
	if err == nil {
		return true, nil
	}
	err = parseError(err)
	if NotExist.MatchString(err.Error()) {
		return false, nil
	}

	return false, parseError(err)
}

func (z zfsEntryBase) Receive() (runcmd.CmdWorker, error) {
	c, err := z.runner.Command("zfs receive " + z.Path)
	if err != nil {
		return nil, err
	}

	return c, c.Start()
}

func (z zfsEntryBase) GetPool() string {
	buf := strings.SplitN(z.Path, "/", 2)
	return buf[0]
}

func (z zfsEntryBase) GetLastPath() string {
	buf := strings.Split(z.Path, "/")
	return buf[len(buf)-1]
}

type Fs struct {
	zfsEntryBase
}

// See Zfs.CreateFs
func CreateFs(zfsPath string) (Fs, error) {
	return std.CreateFs(zfsPath)
}

// Actually creates filesystem
func (z Zfs) CreateFs(zfsPath string) (Fs, error) {
	fs := NewFs(zfsPath)
	ok, err := fs.Exists()
	if err != nil {
		return Fs{}, err
	}
	if ok {
		return Fs{}, errors.New(fmt.Sprintf("fs %s already exists", zfsPath))
	}
	c, err := z.Command("zfs create " + zfsPath)
	if err != nil {
		return z.NewFs(zfsPath), err
	}
	_, err = c.Run()
	return z.NewFs(zfsPath), parseError(err)
}

// See Zfs.NewFs
func NewFs(zfsPath string) Fs {
	return std.NewFs(zfsPath)
}

// Return Fs wrapper without any checks and actualy creation
func (z Zfs) NewFs(zfsPath string) Fs {
	return Fs{zfsEntryBase{z, zfsPath}}
}

// See Zfs.ListFs
func ListFs(path string) ([]Fs, error) {
	return std.ListFs(path)
}

// Return list of all found filesystems
func (z Zfs) ListFs(path string) ([]Fs, error) {
	c, err := z.Command("zfs list -Hr -o name " + path)
	if err != nil {
		return []Fs{}, errors.New("error listing fs: " + err.Error())
	}

	out, err := c.Run()
	if err != nil {
		err := parseError(err)
		if NotExist.MatchString(err.Error()) {
			return []Fs{}, nil
		}

		return []Fs{}, parseError(err)
	}

	filesystems := []Fs{}
	for _, fs := range out {
		filesystems = append(filesystems, z.NewFs(fs))
	}

	return filesystems, nil
}

func (f Fs) Promote() error {
	c, err := f.runner.Command("zfs promote " + f.Path)
	if err != nil {
		return errors.New("error promoting fs: " + err.Error())
	}
	_, err = c.Run()
	if err != nil {
		return parseError(err)
	}

	return nil
}
