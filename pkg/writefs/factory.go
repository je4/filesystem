package writefs

import (
	"emperror.dev/errors"
	"golang.org/x/exp/slices"
	"io/fs"
	"regexp"
)

type levelFS uint8

const (
	LowFS levelFS = iota
	MediumFS
	HighFS
)

type CreateFSFunc func(f *Factory, path string) (fs.FS, error)

type createFS struct {
	level  levelFS
	re     *regexp.Regexp
	create CreateFSFunc
}

type Factory struct {
	fss []*createFS
}

func NewFactory() (*Factory, error) {
	f := &Factory{fss: []*createFS{}}
	return f, nil
}

func (f *Factory) Register(create CreateFSFunc, prefixRegexp string, level levelFS) error {
	re, err := regexp.Compile(prefixRegexp)
	if err != nil {
		return errors.Wrapf(err, "cannot compile regexp '%s'", prefixRegexp)
	}
	// insert new createFS in order of level
	cs := &createFS{
		level:  level,
		re:     re,
		create: create,
	}
	pos, _ := slices.BinarySearchFunc(f.fss, cs, func(a, b *createFS) int {
		if a.level > b.level {
			return -1
		}
		if a.level < b.level {
			return 1
		}
		return 0
	})
	f.fss = append(f.fss, nil)
	copy(f.fss[pos+1:], f.fss[pos:])
	f.fss[pos] = cs
	return nil
}

func (f *Factory) Get(path string) (fs.FS, error) {
	for _, cs := range f.fss {
		if cs.re.MatchString(path) {
			fsys, err := cs.create(f, path)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot create filesystem for '%s'", path)
			}
			return fsys, nil
		}
	}
	return nil, errors.Errorf("path %s not supported", path)
}
