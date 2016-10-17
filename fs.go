package zfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/theairkit/runcmd"
)

type RecursiveFlag int

const (
	RF_No RecursiveFlag = iota
	RF_Soft
	RF_Hard
)

type ZfsEntry interface {
	GetProperty(string) (string, error)
	GetPropertyInt(string) (int64, error)
	SetProperty(string, string) error
	GetPool() string
	GetLastPath() string
	Destroy(RecursiveFlag) error
	Exists() (bool, error)
	Receive() (runcmd.CmdWorker, io.WriteCloser, error)
	getPath() string
}

type zfsEntryBase struct {
	runner Zfs
	Path   string
}

func (z zfsEntryBase) GetProperty(prop string) (string, error) {
	c := z.runner.Command("zfs", "get", "-Hp", "-o", "value", prop, z.Path)

	stdout, stderr, err := c.Output()
	if err != nil {
		return "", parseError(err, stderr)
	}
	return strings.Split(string(stdout), "\n")[0], nil
}

func (z zfsEntryBase) GetPropertyInt(prop string) (int64, error) {
	c := z.runner.Command("zfs", "get", "-Hp", "-o", "value", prop, z.Path)

	stdout, stderr, err := c.Output()
	if err != nil {
		return 0, parseError(err, stderr)
	}
	val, err := strconv.ParseInt(strings.Split(string(stdout), "\n")[0], 10, 64)
	if err != nil {
		return 0, errors.New("error converting to int: " + err.Error())
	}
	return val, nil
}

func (z zfsEntryBase) getPath() string {
	return z.Path
}

func (z zfsEntryBase) SetProperty(prop, value string) error {
	c := z.runner.Command("zfs", "set", prop+"="+value, z.Path)

	if _, stderr, err := c.Output(); err != nil {
		return parseError(err, stderr)
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

func (z zfsEntryBase) Destroy(recursive RecursiveFlag) error {
	args := []string{"destroy"}

	switch recursive {
	case RF_Soft:
		args = append(args, "-r")
	case RF_Hard:
		args = append(args, "-R")
	}

	args = append(args, z.Path)

	c := z.runner.Command("zfs", args...)

	_, stderr, err := c.Output()
	return parseError(err, stderr)
}

func (z zfsEntryBase) Exists() (bool, error) {
	c := z.runner.Command("zfs", "list", "-H", "-o", "name", z.Path)

	stdout, stderr, err := c.Output()

	if err == nil && strings.Split(string(stdout), "\n")[0] == z.Path {
		return true, nil
	}

	err = parseError(err, stderr)
	if err != nil && NotExist.MatchString(err.Error()) {
		return false, nil
	}

	return false, err
}

func (z zfsEntryBase) Receive() (runcmd.CmdWorker, io.WriteCloser, error) {
	c := z.runner.Command("zfs", "create", "-p", z.Path)

	_, stderr, err := c.Output()
	if err != nil {
		return nil, nil, parseError(err, stderr)
	}

	c = z.runner.Command("zfs", "receive", "-F", z.Path)

	stdinPipe, err := c.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	return c, stdinPipe, c.Start()
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
		return z.NewFs(zfsPath), err
	}
	if ok {
		return z.NewFs(zfsPath), errors.New(fmt.Sprintf("fs %s already exists", zfsPath))
	}

	c := z.Command("zfs", "create", "-p", zfsPath)

	_, stderr, err := c.Output()
	return z.NewFs(zfsPath), parseError(err, stderr)
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
	c := z.Command("zfs", "list", "-Hr", "-o", "name", path)

	stdout, stderr, err := c.Output()
	if err != nil {
		err := parseError(err, stderr)
		if NotExist.MatchString(err.Error()) {
			return []Fs{}, nil
		}

		return []Fs{}, parseError(err, stderr)
	}

	filesystems := []Fs{}
	for _, fs := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		if fs == "" {
			continue
		}
		filesystems = append(filesystems, z.NewFs(fs))
	}

	return filesystems, nil
}

func (f Fs) Promote() error {
	c := f.runner.Command("zfs", "promote", f.Path)

	_, stderr, err := c.Output()
	return parseError(err, stderr)
}

func (f Fs) Mount() error {
	c := f.runner.Command("zfs", "mount", f.Path)

	_, stderr, err := c.Output()
	return parseError(err, stderr)
}

func (f Fs) Unmount() error {
	c := f.runner.Command("zfs", "unmount", f.Path)

	_, stderr, err := c.Output()
	return parseError(err, stderr)
}
