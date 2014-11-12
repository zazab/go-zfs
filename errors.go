package zfs

import (
	"errors"
	"regexp"
	"strings"
)

var (
	BadPropGet      = regexp.MustCompile(`bad property list: invalid property '.+'`)
	BadPropSet      = regexp.MustCompile(`cannot set property for '.+': invalid property '.+'`)
	NeedRec         = regexp.MustCompile(`cannot destroy '.+': filesystem has children`)
	NotExist        = regexp.MustCompile(`cannot open '.+': dataset does not exist`)
	NotMounted      = regexp.MustCompile(`filesystem successfully created, but not mounted`)
	NeedSudo        = regexp.MustCompile(`need sudo`)
	AllreadyExists  = regexp.MustCompile(`fs .+ already exists`)
	PromoteNotClone = regexp.MustCompile(`cannot promote '.+': not a cloned filesystem`)
	InvalidDataset  = regexp.MustCompile(`cannot open '.+': invalid dataset name`)

	PoolError = errors.New("error creating clone: source and target in different pools")
)

func joinErrs(errs []string) string {
	return strings.Join(errs, "; ")
}

func parseError(err error) error {
	if err == nil {
		return nil
	}

	errs := strings.Split(err.Error(), "\n")

	if errs[0] == "exit status 1" {
		errs = errs[1:]
	}

	if errs[0] == "exit status 2" {
		errs = errs[1:]
	}

	if errs[len(errs)-1] == "" {
		errs = errs[:len(errs)-1]
	}

	switch {
	case BadPropGet.MatchString(errs[0]):
		return errors.New(errs[0])
	case NeedRec.MatchString(errs[0]):
		return errors.New(errs[0])
	case NotExist.MatchString(errs[0]):
		return errors.New(errs[0])
	case NotMounted.MatchString(errs[len(errs)-1]):
		return errors.New(errs[len(errs)-1] + "need sudo to mount")
	case InvalidDataset.MatchString(errs[0]):
		return errors.New(errs[0])
	default:
		return errors.New(joinErrs(errs))
	}
}
