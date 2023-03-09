package db

type Options struct {
	DBPath        string
	FileSizeLimit int64
}

func DefaultOptions(path string) *Options {
	return &Options{
		DBPath:        path,
		FileSizeLimit: 1 << 26, // 默认64M
	}
}
