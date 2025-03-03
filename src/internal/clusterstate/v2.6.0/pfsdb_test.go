//go:build unit_test

package v2_6_0

import (
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/client"
	v2_5_0 "github.com/pachyderm/pachyderm/v2/src/internal/clusterstate/v2.5.0"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
)

func TestValidateOldDAGs(t *testing.T) {
	type testCase struct {
		cis       []*v2_5_0.CommitInfo
		expectErr bool
	}
	makeCommit := func(c *pfs.Commit, parent *pfs.Commit, kind pfs.OriginKind) *v2_5_0.CommitInfo {
		return &v2_5_0.CommitInfo{
			Commit:       c,
			ParentCommit: parent,
			Origin:       &pfs.CommitOrigin{Kind: kind},
		}
	}
	p := pfs.DefaultProjectName
	r1 := "repo1"
	r2 := "repo2"
	b1 := "branch1"
	b2 := "branch2"
	id1 := "abc"
	r1Stub := makeCommit(client.NewProjectCommit(p, r1, b1, "xyz"), nil, pfs.OriginKind_AUTO)
	r2Stub := makeCommit(client.NewProjectCommit(p, r2, b1, "xyz"), nil, pfs.OriginKind_AUTO)
	cases := []testCase{
		{
			cis: []*v2_5_0.CommitInfo{
				makeCommit(client.NewProjectCommit(p, r1, b1, id1), r1Stub.Commit, pfs.OriginKind_AUTO),
				makeCommit(client.NewProjectCommit(p, r2, b1, id1), r2Stub.Commit, pfs.OriginKind_AUTO),
			},
			expectErr: false,
		},
		{
			cis: []*v2_5_0.CommitInfo{
				makeCommit(client.NewProjectCommit(p, r1, b1, id1), r1Stub.Commit, pfs.OriginKind_AUTO),
				makeCommit(client.NewProjectCommit(p, r1, b2, id1), client.NewProjectCommit(p, r1, b1, id1), pfs.OriginKind_ALIAS),
			},
			expectErr: false,
		},
		{
			cis: []*v2_5_0.CommitInfo{
				makeCommit(client.NewProjectCommit(p, r1, b1, id1), r1Stub.Commit, pfs.OriginKind_AUTO),
				makeCommit(client.NewProjectCommit(p, r1, b2, id1), r1Stub.Commit, pfs.OriginKind_ALIAS),
			},
			expectErr: true,
		},
	}
	for _, c := range cases {
		err := validateExistingDAGs(c.cis)
		if c.expectErr {
			require.YesError(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
