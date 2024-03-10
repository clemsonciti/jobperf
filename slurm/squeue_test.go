package slurm

import "testing"

func TestNGPUsFromGRESDetails(t *testing.T) {
	for _, tc := range []struct {
		input        string
		expectedNGPU int
		expectedErr  bool
	}{
		{
			input:        "gpu:a100:2(IDX:0-1)",
			expectedNGPU: 2,
		}, {
			input:        "gpu:a100:1(IDX:0)",
			expectedNGPU: 1,
		}, {
			input:        "gpu:a100:1(IDX:0),gpu:a100:2(IDX:0-1)",
			expectedNGPU: 3,
		}, {
			input:        "gpu:1(IDX:0)",
			expectedNGPU: 1,
		}, {
			input:        "foo:1(IDX:0)",
			expectedNGPU: 0,
		}, {
			input:        "foo:blah:1(IDX:0)",
			expectedNGPU: 0,
		}, {
			input:        "foo:blah:1(IDX:0),gpu:1(IDX:0)",
			expectedNGPU: 1,
		}, {
			input:        "gpu(CNT:3)",
			expectedNGPU: 3,
		}, {
			input:        "gpu:a100(CNT:3)",
			expectedNGPU: 3,
		}, {
			input:       "1(IDX:0-3)",
			expectedErr: true,
		}, {
			input:       "",
			expectedErr: true,
		}, {
			input:       "gpu:a100",
			expectedErr: true,
		},
	} {
		n, err := nGPUFromGRESDetails(tc.input)
		if tc.expectedErr {
			if err == nil {
				t.Errorf("for input %v, expected error", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("for input %v, expected no error, but got %v", tc.input, err)
			}
			if tc.expectedNGPU != n {
				t.Errorf("for input %v, expected %v for ngpu, got %v", tc.input, tc.expectedNGPU, n)
			}
		}
	}
}
