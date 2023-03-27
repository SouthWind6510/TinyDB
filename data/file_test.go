package data

import (
	"reflect"
	"testing"
)

func Test_getFileName(t *testing.T) {
	type args struct {
		path     string
		fid      int16
		fileType DataType
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "string type file",
			args: args{
				path:     "/home/tmp",
				fid:      1,
				fileType: String,
			},
			want: "/home/tmp/1.str.log",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFileName(tt.args.path, tt.args.fid, tt.args.fileType); got != tt.want {
				t.Errorf("getFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenDataFile(t *testing.T) {
	type args struct {
		path     string
		fid      int16
		fileType DataType
		fileSize int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "small file",
			args: args{
				path:     "/Users/southwind/TinyDB/test/0",
				fid:      1,
				fileType: String,
				fileSize: 1 << 10,
			},
			wantErr: false,
		},
		{
			name: "mid file",
			args: args{
				path:     "/Users/southwind/TinyDB/test/0",
				fid:      2,
				fileType: String,
				fileSize: 1 << 20,
			},
			wantErr: false,
		}, {
			name: "big file",
			args: args{
				path:     "/Users/southwind/TinyDB/test/0",
				fid:      3,
				fileType: String,
				fileSize: 1 << 30,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := OpenDataFile(tt.args.path, tt.args.fid, tt.args.fileType, tt.args.fileSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenDataFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			_ = file.Close()
			_ = file.Remove()
		})
	}
}

func TestDataFile_WriteReadEntry(t *testing.T) {
	type args struct {
		path     string
		fid      int16
		fileType DataType
		fileSize int64
	}
	tests := []struct {
		name      string
		args      args
		wantEntry *Entry
		wantErr   bool
	}{
		{
			name: "test 1",
			args: args{
				path:     "/Users/southwind/TinyDB/test/0",
				fid:      1,
				fileType: String,
				fileSize: 1 << 10,
			},
			wantEntry: NewEntry([]byte("hello"), []byte("world"), Insert),
		}, {
			name: "test 2",
			args: args{
				path:     "/Users/southwind/TinyDB/test/0",
				fid:      1,
				fileType: String,
				fileSize: 1 << 10,
			},
			wantEntry: NewEntry([]byte("疯狂星期四"), []byte("v我50"), Insert),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df, _ := OpenDataFile(tt.args.path, tt.args.fid, tt.args.fileType, tt.args.fileSize)
			err := df.Write(EncodeEntry(tt.wantEntry))
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotEntry, err := df.ReadEntry(0)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotEntry, tt.wantEntry) {
				t.Errorf("ReadEntry() gotEntry = %v, want %v", gotEntry, tt.wantEntry)
			}
			//_ = df.Remove()
		})
	}
}
