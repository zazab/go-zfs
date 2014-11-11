package zfs

import "errors"

type Snapshot struct {
	zfsEntry
	Fs   Fs
	Name string
}

func (s Snapshot) Clone(targetPath string) (Fs, error) {
	if s.GetPool() != NewFs(targetPath).GetPool() {
		return Fs{}, errors.New("error creating clone: source and target in different pools")
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
