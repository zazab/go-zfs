package zfs

import (
	"errors"
	"regexp"
	"strings"
)

var (
	BadPropGet         = regexp.MustCompile(`bad property list: invalid property '.+'$`)
	BadPropSet         = regexp.MustCompile(`cannot set property for '.+': invalid property '.+'$`)
	NeedRec            = regexp.MustCompile(`cannot destroy '.+': filesystem has children$`)
	NotExist           = regexp.MustCompile(`cannot open '.+': dataset does not exist$`)
	NotMounted         = regexp.MustCompile(`^filesystem successfully created, but not mounted`)
	NeedSudo           = regexp.MustCompile(`need sudo`)
	AllreadyExists     = regexp.MustCompile(`fs .+ already exists$`)
	PromoteNotClone    = regexp.MustCompile(`cannot promote '.+': not a cloned filesystem$`)
	InvalidDataset     = regexp.MustCompile(`invalid( dataset)? name$`)
	ReceiverExists     = regexp.MustCompile(`cannot receive new filesystem stream: destination '.+' exists$`)
	MostRecentNotMatch = regexp.MustCompile(`cannot receive incremental stream: most recent snapshot of '.+' does not`)
	BrokenPipe         = regexp.MustCompile(`broken pipe$`)

	PoolError = errors.New("error creating clone: source and target in different pools")
)

func joinErrs(errs []string) string {
	return strings.Join(errs, "; ")
}

func parseError(err error, stderr []byte) error {
	if err == nil {
		return nil
	}

	var errs []string
	if stderr != nil {
		errs = strings.Split(string(stderr), "\n")
	} else {
		errs = strings.Split(string(err.Error()), "\n")
	}

	if strings.Contains(errs[0], "exit status") {
		if len(errs) > 1 {
			errs = errs[1:]
		} else {
			errs = []string{errs[0]}
		}
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
		return errors.New(errs[len(errs)-1] + " need sudo to mount")
	case InvalidDataset.MatchString(errs[0]):
		return errors.New(errs[0])
	case ReceiverExists.MatchString(errs[0]):
		return errors.New(errs[0])
	case MostRecentNotMatch.MatchString(errs[0]):
		return errors.New(strings.Join(errs, " "))
	default:
		return errors.New(joinErrs(errs))
	}
}
