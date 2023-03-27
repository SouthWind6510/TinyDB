package ds

import (
	"testing"
)

// a 1
// b 2
// f 2
// c 3
// d 3
// e 4
func InitSkipList() *SkipList {
	zsl := NewSkipList(2)
	zsl.Insert("a", 1)
	zsl.Insert("b", 2)
	zsl.Insert("c", 3)
	zsl.Insert("d", 3)
	zsl.Insert("e", 4)
	zsl.Insert("f", 2)
	zsl.Delete("b", 2)
	zsl.Insert("b", 2)

	return zsl
}

func Test_skipList_GetRank(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		member string
		score  float64
	}
	tests := []struct {
		name     string
		args     args
		wantRank int64
	}{
		{
			name: "get rank test1",
			args: args{
				member: "a",
				score:  1,
			},
			wantRank: 1,
		},
		{
			name: "get rank test2",
			args: args{
				member: "f",
				score:  2,
			},
			wantRank: 3,
		},
		{
			name: "get rank test3",
			args: args{
				member: "c",
				score:  3,
			},
			wantRank: 4,
		},
		{
			name: "get rank test4",
			args: args{
				member: "d",
				score:  3,
			},
			wantRank: 5,
		},
		{
			name: "get rank test5",
			args: args{
				member: "c",
				score:  4,
			},
			wantRank: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRank := zsl.GetRank(tt.args.member, tt.args.score); gotRank != tt.wantRank {
				t.Errorf("GetRank() = %v, want %v", gotRank, tt.wantRank)
			}
		})
	}
}

func Test_zskipList_RandLevel(t *testing.T) {
	type fields struct {
		length   int64
		skipSpan int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "rand level test1",
			fields: fields{
				8, 2,
			},
			want: 4,
		},
		{
			name: "rand level test2",
			fields: fields{
				5, 2,
			},
			want: 3,
		},
		{
			name: "rand level test3",
			fields: fields{
				1, 2,
			},
			want: 1,
		}, {
			name: "rand level test4",
			fields: fields{
				9, 3,
			},
			want: 3,
		},
	}
	TestCount := 100
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zsl := &SkipList{
				length:   tt.fields.length,
				skipSpan: tt.fields.skipSpan,
			}
			for i := 0; i < TestCount; i++ {
				if got := zsl.RandLevel(); got > tt.want {
					t.Errorf("RandLevel() = %v, want <= %v", got, tt.want)
				}
			}
		})
	}
}

func TestSkipList_IsInRange(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		min float64
		max float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "恰好包含值域",
			args: args{
				min: 1,
				max: 4,
			},
			want: true,
		}, {
			name: "包含最小值",
			args: args{
				min: 0,
				max: 2,
			},
			want: true,
		}, {
			name: "包含最大值",
			args: args{
				min: 2,
				max: 5,
			},
			want: true,
		}, {
			name: "包含值域",
			args: args{
				min: 0,
				max: 5,
			},
			want: true,
		}, {
			name: "大于值域",
			args: args{
				min: 5,
				max: 6,
			},
			want: false,
		}, {
			name: "非法参数",
			args: args{
				min: 2,
				max: 1,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := zsl.IsInRange(tt.args.min, tt.args.max); got != tt.want {
				t.Errorf("IsInRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSkipList_FirstInRange(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		min float64
		max float64
	}
	tests := []struct {
		name       string
		args       args
		wantMember string
		wantScore  float64
	}{
		{
			name: "恰好包含值域",
			args: args{
				min: 1,
				max: 3,
			},
			wantMember: "a",
			wantScore:  1,
		},
		{
			name: "包含最小值",
			args: args{
				min: 0,
				max: 2,
			},
			wantMember: "a",
			wantScore:  1,
		}, {
			name: "包含最大值",
			args: args{
				min: 2,
				max: 4,
			},
			wantMember: "b",
			wantScore:  2,
		}, {
			name: "score相等",
			args: args{
				min: 3,
				max: 3,
			},
			wantMember: "c",
			wantScore:  3,
		}, {
			name: "不存在",
			args: args{
				min: 5,
				max: 6,
			},
			wantMember: "",
			wantScore:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := zsl.FirstInRange(tt.args.min, tt.args.max)
			if tt.wantMember == "" && got != nil {
				t.Errorf("FirstInRange() = %v, want nil", got)
			} else if tt.wantMember != "" && (got.score != tt.wantScore || got.member != tt.wantMember) {
				t.Errorf("FirstInRange() = %v, want member: %v, want score: %v", got, tt.wantMember, tt.wantScore)
			}
		})
	}
}

func TestSkipList_LastInRange(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		min float64
		max float64
	}
	tests := []struct {
		name       string
		args       args
		wantMember string
		wantScore  float64
	}{
		{
			name: "恰好包含值域",
			args: args{
				min: 1,
				max: 3,
			},
			wantMember: "d",
			wantScore:  3,
		},
		{
			name: "包含最小值",
			args: args{
				min: 0,
				max: 2,
			},
			wantMember: "f",
			wantScore:  2,
		}, {
			name: "包含最大值",
			args: args{
				min: 2,
				max: 5,
			},
			wantMember: "e",
			wantScore:  4,
		}, {
			name: "score相等",
			args: args{
				min: 3,
				max: 3,
			},
			wantMember: "d",
			wantScore:  3,
		}, {
			name: "不存在",
			args: args{
				min: 5,
				max: 6,
			},
			wantMember: "",
			wantScore:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := zsl.LastInRange(tt.args.min, tt.args.max)
			if tt.wantMember == "" && got != nil {
				t.Errorf("LastInRange() = %v, want nil", got)
			} else if tt.wantMember != "" && (got.score != tt.wantScore || got.member != tt.wantMember) {
				t.Errorf("LastInRange() = %v, want member: %v, want score: %v", got, tt.wantMember, tt.wantScore)
			}
		})
	}
}

func TestSkipList_GetElementByRank(t *testing.T) {
	zsl := InitSkipList()

	tests := []struct {
		name       string
		rank       int64
		wantMember string
	}{
		{
			name:       "rank 1",
			rank:       1,
			wantMember: "a",
		}, {
			name:       "rank 3",
			rank:       3,
			wantMember: "f",
		}, {
			name:       "rank 6",
			rank:       6,
			wantMember: "e",
		}, {
			name:       "rank 7",
			rank:       7,
			wantMember: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := zsl.GetElementByRank(tt.rank)
			if tt.wantMember == "" && got != nil {
				t.Errorf("GetElementByRank() = %v, want nil", got)
			} else if tt.wantMember != "" && got.GetMember() != tt.wantMember {
				t.Errorf("GetElementByRank() = %v, want member %v", got.GetMember(), tt.wantMember)
			}
		})
	}
}

func TestSkipList_GetRangeByRank(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		start int64
		end   int64
	}
	tests := []struct {
		name       string
		args       args
		wantLength int
	}{
		{
			name: "start 1, end 3",
			args: args{
				start: 1,
				end:   3,
			},
			wantLength: 3,
		}, {
			name: "start 1, end 7",
			args: args{
				start: 1,
				end:   7,
			},
			wantLength: 6,
		}, {
			name: "start 2, end 7",
			args: args{
				start: 2,
				end:   7,
			},
			wantLength: 5,
		}, {
			name: "start 3, end 3",
			args: args{
				start: 3,
				end:   3,
			},
			wantLength: 1,
		}, {
			name: "start 7, end 8",
			args: args{
				start: 7,
				end:   8,
			},
			wantLength: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotNodes := zsl.GetRangeByRank(tt.args.start, tt.args.end); len(gotNodes) != tt.wantLength {
				t.Errorf("GetRangeByRank() length = %v, want length%v", len(gotNodes), tt.wantLength)
			}
		})
	}
}

func TestSkipList_GetRangeByScore(t *testing.T) {
	zsl := InitSkipList()

	type args struct {
		min float64
		max float64
	}
	tests := []struct {
		name       string
		args       args
		wantLength int
	}{
		{
			name: "min 0, max 2",
			args: args{
				min: 0,
				max: 2,
			},
			wantLength: 3,
		}, {
			name: "min 1, max 4",
			args: args{
				min: 1,
				max: 4,
			},
			wantLength: 6,
		}, {
			name: "min 2, max 4",
			args: args{
				min: 2,
				max: 4,
			},
			wantLength: 5,
		}, {
			name: "min 5, max 6",
			args: args{
				min: 5,
				max: 6,
			},
			wantLength: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotNodes := zsl.GetRangeByScore(tt.args.min, tt.args.max); len(gotNodes) != tt.wantLength {
				t.Errorf("GetRangeByScore() length = %v, want length %v", gotNodes, tt.wantLength)
			}
		})
	}
}

func TestSkipList_DeleteRangeByScore(t *testing.T) {
	type args struct {
		min float64
		max float64
	}
	tests := []struct {
		name        string
		args        args
		wantDeleted int64
	}{
		{
			name: "全删",
			args: args{
				min: 1,
				max: 4,
			},
			wantDeleted: 6,
		}, {
			name: "包含左边界",
			args: args{
				min: 0,
				max: 2,
			},
			wantDeleted: 3,
		}, {
			name: "包含右边界",
			args: args{
				min: 3,
				max: 4,
			},
			wantDeleted: 3,
		}, {
			name: "大区间",
			args: args{
				min: 0,
				max: 5,
			},
			wantDeleted: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zsl := InitSkipList()
			if gotDeleted := zsl.DeleteRangeByScore(tt.args.min, tt.args.max); int64(len(gotDeleted)) != tt.wantDeleted {
				t.Errorf("DeleteRangeByScore() = %v, want %v", gotDeleted, tt.wantDeleted)
			}
		})
	}
}
