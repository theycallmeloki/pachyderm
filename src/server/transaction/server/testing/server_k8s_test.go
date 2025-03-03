//go:build k8s

package testing

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/minikubetestenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
)

func TestCreatePipelineTransaction(t *testing.T) {
	c, _ := minikubetestenv.AcquireCluster(t)
	repo := testutil.UniqueString("in")
	pipeline := testutil.UniqueString("pipeline")
	_, err := c.ExecuteInTransaction(func(txnClient *client.APIClient) error {
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, repo))
		require.NoError(t, txnClient.CreateProjectPipeline(pfs.DefaultProjectName,
			pipeline,
			"",
			[]string{"bash"},
			[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out", repo)},
			&pps.ParallelismSpec{Constant: 1},
			client.NewProjectPFSInput(pfs.DefaultProjectName, repo, "/"),
			"master",
			false,
		))
		return nil
	})
	require.NoError(t, err)

	commit := client.NewProjectCommit(pfs.DefaultProjectName, repo, "master", "")
	require.NoError(t, c.PutFile(commit, "foo", strings.NewReader("bar")))

	commitInfo, err := c.WaitProjectCommit(pfs.DefaultProjectName, pipeline, "master", "")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "foo", &buf))
	require.Equal(t, "bar", buf.String())
}

func TestCreateProjectlessPipelineTransaction(t *testing.T) {
	c, _ := minikubetestenv.AcquireCluster(t)
	repo := testutil.UniqueString("in")
	pipeline := testutil.UniqueString("pipeline")
	_, err := c.ExecuteInTransaction(func(txnClient *client.APIClient) error {
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, repo))
		_, err := txnClient.PpsAPIClient.CreatePipeline(txnClient.Ctx(),
			&pps.CreatePipelineRequest{
				Pipeline: &pps.Pipeline{Name: pipeline},
				Transform: &pps.Transform{
					Image: testutil.DefaultTransformImage,
					Cmd:   []string{"bash"},
					Stdin: []string{fmt.Sprintf("cp /pfs/%s/* /pfs/out", repo)},
				},
				ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
				Input:           client.NewProjectPFSInput(pfs.DefaultProjectName, repo, "/"),
				OutputBranch:    "master",
			})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	commit := client.NewProjectCommit(pfs.DefaultProjectName, repo, "master", "")
	require.NoError(t, c.PutFile(commit, "foo", strings.NewReader("bar")))

	commitInfo, err := c.WaitProjectCommit(pfs.DefaultProjectName, pipeline, "master", "")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "foo", &buf))
	require.Equal(t, "bar", buf.String())
}
