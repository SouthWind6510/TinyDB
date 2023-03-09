package data

import (
	. "SouthWind6510/TinyDB/pkg/logger"
	"reflect"
	"testing"
)

func Test_encodeEntry(t *testing.T) {
	type args struct {
		e *Entry
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				NewEntry([]byte("key"), []byte("value"), INSERT),
			},
		},
		{
			name: "test2",
			args: args{
				NewEntry([]byte("疯狂星期四"), []byte("v我50"), INSERT),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBuf := EncodeEntry(tt.args.e)
			gotEntry := decodeEntry(gotBuf)
			Log.Infof("decode: %v", gotEntry)
			if !reflect.DeepEqual(tt.args.e.Key, gotEntry.Key) || !reflect.DeepEqual(tt.args.e.Value, gotEntry.Value) {
				t.Errorf("decodeEntry = %+v, want: %+v", gotEntry, tt.args.e)
			}
		})
	}
}
