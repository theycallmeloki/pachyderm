package testing

import (
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/internal/pctx"
	"github.com/pachyderm/pachyderm/v2/src/internal/testpachd/realenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/dockertestenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/transaction"
)

func requireEmptyResponse(t *testing.T, response *transaction.TransactionResponse) {
	require.Nil(t, response.Commit)
}

func requireCommitResponse(t *testing.T, response *transaction.TransactionResponse, commit *pfs.Commit) {
	require.Equal(t, commit, response.Commit)
}

func TestTransactions(suite *testing.T) {
	suite.Parallel()
	suite.Run("TestEmptyTransaction", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		info, err := env.PachClient.InspectTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 0, len(info.Requests))
		require.Equal(t, 0, len(info.Responses))
		require.NotNil(t, info.Started)

		info, err = env.PachClient.FinishTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 0, len(info.Requests))
		require.Equal(t, 0, len(info.Responses))
		require.NotNil(t, info.Started)

		info, err = env.PachClient.InspectTransaction(txn)
		require.YesError(t, err)
		require.Nil(t, info)
	})

	suite.Run("TestInvalidatedTransaction", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)
		createRepo := &pfs.CreateRepoRequest{
			Repo: client.NewProjectRepo(pfs.DefaultProjectName, "foo"),
		}

		// Tell the transaction to create a repo
		_, err = txnClient.PfsAPIClient.CreateRepo(txnClient.Ctx(), createRepo)
		require.NoError(t, err)

		// Create the same repo outside of the transaction, so it can't run
		_, err = env.PachClient.PfsAPIClient.CreateRepo(env.Context, createRepo)
		require.NoError(t, err)

		// Finishing the transaction should fail
		info, err := env.PachClient.FinishTransaction(txn)
		require.YesError(t, err)
		require.Nil(t, info)

		// Appending to the transaction should fail
		_, err = txnClient.PfsAPIClient.CreateRepo(txnClient.Ctx(), &pfs.CreateRepoRequest{Repo: client.NewProjectRepo(pfs.DefaultProjectName, "bar")})
		require.YesError(t, err)
	})

	suite.Run("TestFailedAppend", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)
		createRepo := &pfs.CreateRepoRequest{
			Repo: client.NewProjectRepo(pfs.DefaultProjectName, "foo"),
		}

		// Create a repo outside of a transaction
		_, err = env.PachClient.PfsAPIClient.CreateRepo(env.Context, createRepo)
		require.NoError(t, err)

		// Tell the transaction to create the same repo, which should fail
		_, err = txnClient.PfsAPIClient.CreateRepo(txnClient.Ctx(), createRepo)
		require.YesError(t, err)

		info, err := env.PachClient.InspectTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 0, len(info.Requests))
		require.Equal(t, 0, len(info.Responses))

		info, err = env.PachClient.FinishTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 0, len(info.Requests))
		require.Equal(t, 0, len(info.Responses))
	})

	suite.Run("TestDependency", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))
		project := testutil.UniqueString("p-")
		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)

		_, err = txnClient.PfsAPIClient.CreateProject(txnClient.Ctx(), &pfs.CreateProjectRequest{
			Project: client.NewProject(project), // projects is not transactional, so it shouldn't be added to inspect transaction request count
		})
		require.NoError(t, err)
		// Create repo, start commit, finish commit
		_, err = txnClient.PfsAPIClient.CreateRepo(txnClient.Ctx(), &pfs.CreateRepoRequest{
			Repo: client.NewProjectRepo(project, "foo"),
		})
		require.NoError(t, err)

		commit, err := txnClient.PfsAPIClient.StartCommit(txnClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewProjectBranch(project, "foo", "master"),
		})
		require.NoError(t, err)

		_, err = txnClient.PfsAPIClient.FinishCommit(txnClient.Ctx(), &pfs.FinishCommitRequest{
			Commit: client.NewProjectCommit(project, "foo", "master", ""),
		})
		require.NoError(t, err)

		info, err := env.PachClient.InspectTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 3, len(info.Requests))
		require.Equal(t, 3, len(info.Responses))

		// Check each response value
		requireEmptyResponse(t, info.Responses[0])
		requireCommitResponse(t, info.Responses[1], commit)
		requireEmptyResponse(t, info.Responses[2])

		info, err = env.PachClient.FinishTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, txn, info.Transaction)
		require.Equal(t, 3, len(info.Requests))
		require.Equal(t, 3, len(info.Responses))

		// Double-check each response value
		requireEmptyResponse(t, info.Responses[0])
		requireCommitResponse(t, info.Responses[1], commit)
		requireEmptyResponse(t, info.Responses[2])

		info, err = env.PachClient.InspectTransaction(txn)
		require.YesError(t, err)
		require.Nil(t, info)
	})

	// This is a regression test for a bug in PFS where creating a branch would
	// inspect the new commit outside of the transaction STM and fail to find it.
	suite.Run("TestCreateBranch", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		repo := "foo"
		branchA := "master"
		branchB := "bar"
		project := testutil.UniqueString("prj-")

		require.NoError(t, env.PachClient.CreateProject(project))
		require.NoError(t, env.PachClient.CreateProjectRepo(project, repo))
		require.NoError(t, env.PachClient.CreateProjectBranch(project, repo, branchA, "", "", nil))
		require.NoError(t, env.PachClient.CreateProjectBranch(project, repo, branchB, "", "", nil))

		txnClient := env.PachClient.WithTransaction(txn)
		commit, err := txnClient.StartProjectCommit(project, repo, branchB)
		require.NoError(t, err)
		err = txnClient.FinishProjectCommit(project, repo, branchB, "")
		require.NoError(t, err)
		require.NoError(t, txnClient.CreateProjectBranch(project, repo, branchA, branchB, "", nil))

		info, err := txnClient.FinishTransaction(txn)
		require.NoError(t, err)

		// Double-check each response value
		requireCommitResponse(t, info.Responses[0], commit)
		requireEmptyResponse(t, info.Responses[1])
		// Exercise branch reading after transaction
		_, err = env.PachClient.InspectProjectBranch(project, repo, branchA)
		require.NoError(t, err)

		branches, err := env.PachClient.ListProjectBranch(project, repo)
		require.NoError(t, err)
		require.Equal(t, 2, len(branches))

		_, err = env.PachClient.InspectProjectBranch(pfs.DefaultProjectName, repo, branchA)
		require.YesError(t, err, "Inspecting a branch in the wrong project should fail.")

		_, err = env.PachClient.ListProjectBranch(pfs.DefaultProjectName, repo)
		require.YesError(t, err)
		// Exercise commit reading after transaction
		_, err = env.PachClient.InspectProjectCommit(pfs.DefaultProjectName, repo, branchA, "")
		require.YesError(t, err, "Inspecting a commit in the wrong project should fail.")

		commitInfo, err := env.PachClient.InspectProjectCommit(project, repo, branchA, "")
		require.NoError(t, err)
		require.Equal(t, commitInfo.Commit.ID, commit.ID)

		commitInfo, err = env.PachClient.InspectProjectCommit(project, repo, branchB, "")
		require.NoError(t, err)
		require.Equal(t, commitInfo.Commit.ID, commit.ID)
	})

	suite.Run("TestDeleteAllTransactions", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		_, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		_, err = env.PachClient.StartTransaction()
		require.NoError(t, err)

		txns, err := env.PachClient.ListTransaction()
		require.NoError(t, err)
		require.Equal(t, 2, len(txns))

		_, err = env.PachClient.TransactionAPIClient.DeleteAll(env.Context, &transaction.DeleteAllRequest{})
		require.NoError(t, err)

		txns, err = env.PachClient.ListTransaction()
		require.NoError(t, err)
		require.Equal(t, 0, len(txns))
	})

	suite.Run("TestMultiCommit", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		project := testutil.UniqueString("prj_")
		err := env.PachClient.CreateProject(project)
		require.NoError(t, err)

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)

		err = txnClient.CreateProjectRepo(project, "foo")
		require.NoError(t, err)

		_, err = txnClient.StartProjectCommit(project, "foo", "master")
		require.NoError(t, err)
		err = txnClient.FinishProjectCommit(project, "foo", "master", "")
		require.NoError(t, err)

		_, err = txnClient.StartProjectCommit(project, "foo", "master")
		require.YesError(t, err)
		require.Matches(t, "already has a commit in this transaction", err.Error())
		// Delete and verify deletion occurs as well
		txns, err := env.PachClient.ListTransaction()
		require.NoError(t, err)
		require.Equal(t, 1, len(txns))

		err = txnClient.DeleteTransaction(txn)
		require.NoError(t, err)

		txns, err = env.PachClient.ListTransaction()
		require.NoError(t, err)
		require.Equal(t, 0, len(txns))

	})

	// Test that a transactional change to multiple repos will only propagate a
	// single commit into a downstream repo. This mimics the pfs.TestProvenance test
	// using the following DAG:
	//  A ─▶ B ─▶ C ─▶ D
	//            ▲
	//  E ────────╯
	suite.Run("TestPropagateCommit", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateProjectRepo(pfs.DefaultProjectName, "A"))
		require.NoError(t, env.PachClient.CreateProjectRepo(pfs.DefaultProjectName, "B"))
		require.NoError(t, env.PachClient.CreateProjectRepo(pfs.DefaultProjectName, "C"))
		require.NoError(t, env.PachClient.CreateProjectRepo(pfs.DefaultProjectName, "D"))
		require.NoError(t, env.PachClient.CreateProjectRepo(pfs.DefaultProjectName, "E"))

		require.NoError(t, env.PachClient.CreateProjectBranch(pfs.DefaultProjectName, "B", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "A", "master")}))
		require.NoError(t, env.PachClient.CreateProjectBranch(pfs.DefaultProjectName, "C", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "B", "master"), client.NewProjectBranch(pfs.DefaultProjectName, "E", "master")}))
		require.NoError(t, env.PachClient.CreateProjectBranch(pfs.DefaultProjectName, "D", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "C", "master")}))

		commitInfos, err := env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "A"))
		require.NoError(t, err)
		aCommits := len(commitInfos)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "B"))
		require.NoError(t, err)
		bCommits := len(commitInfos)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "C"))
		require.NoError(t, err)
		cCommits := len(commitInfos)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "D"))
		require.NoError(t, err)
		dCommits := len(commitInfos)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "E"))
		require.NoError(t, err)
		eCommits := len(commitInfos)

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)

		commitA, err := txnClient.StartProjectCommit(pfs.DefaultProjectName, "A", "master")
		require.NoError(t, err)
		require.NoError(t, txnClient.FinishProjectCommit(pfs.DefaultProjectName, "A", "master", ""))
		require.Equal(t, txn.ID, commitA.ID)
		commitE, err := txnClient.StartProjectCommit(pfs.DefaultProjectName, "E", "master")
		require.NoError(t, err)
		require.NoError(t, txnClient.FinishProjectCommit(pfs.DefaultProjectName, "E", "master", ""))
		require.Equal(t, txn.ID, commitE.ID)

		info, err := txnClient.FinishTransaction(txn)
		require.NoError(t, err)

		require.Equal(t, 4, len(info.Responses))
		requireCommitResponse(t, info.Responses[0], commitA)
		requireEmptyResponse(t, info.Responses[1])
		requireCommitResponse(t, info.Responses[2], commitE)
		requireEmptyResponse(t, info.Responses[3])

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "A"))
		require.NoError(t, err)
		require.Equal(t, aCommits+1, len(commitInfos))
		commitInfoA := commitInfos[0]

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "B"))
		require.NoError(t, err)
		require.Equal(t, bCommits+1, len(commitInfos))

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "C"))
		require.NoError(t, err)
		require.Equal(t, cCommits+1, len(commitInfos))

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "D"))
		require.NoError(t, err)
		require.Equal(t, dCommits+1, len(commitInfos))

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "E"))
		require.NoError(t, err)
		require.Equal(t, eCommits+1, len(commitInfos))
		commitInfoE := commitInfos[0]

		require.Equal(t, commitA, commitInfoA.Commit)
		require.Equal(t, commitE, commitInfoE.Commit)
	})

	// This test is the same as PropagateCommit except more of the operations are
	// performed within the transaction.
	suite.Run("TestPropagateCommitRedux", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		txn, err := env.PachClient.StartTransaction()
		require.NoError(t, err)

		txnClient := env.PachClient.WithTransaction(txn)

		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, "A"))
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, "B"))
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, "C"))
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, "D"))
		require.NoError(t, txnClient.CreateProjectRepo(pfs.DefaultProjectName, "E"))

		require.NoError(t, txnClient.CreateProjectBranch(pfs.DefaultProjectName, "B", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "A", "master")}))
		require.NoError(t, txnClient.CreateProjectBranch(pfs.DefaultProjectName, "C", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "B", "master"), client.NewProjectBranch(pfs.DefaultProjectName, "E", "master")}))
		require.NoError(t, txnClient.CreateProjectBranch(pfs.DefaultProjectName, "D", "master", "", "", []*pfs.Branch{client.NewProjectBranch(pfs.DefaultProjectName, "C", "master")}))

		info, err := txnClient.FinishTransaction(txn)
		require.NoError(t, err)
		require.Equal(t, 8, len(info.Responses))

		commitInfos, err := env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "A"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, txn.ID, commitInfos[0].Commit.ID)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "B"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, txn.ID, commitInfos[0].Commit.ID)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "C"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, txn.ID, commitInfos[0].Commit.ID)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "D"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, txn.ID, commitInfos[0].Commit.ID)

		commitInfos, err = env.PachClient.ListCommitByRepo(client.NewProjectRepo(pfs.DefaultProjectName, "E"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, txn.ID, commitInfos[0].Commit.ID)
	})

	suite.Run("TestBatchTransaction", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))

		var branchInfos []*pfs.BranchInfo
		var info *transaction.TransactionInfo
		var err error

		getBranchNames := func(branchInfos []*pfs.BranchInfo) (result []string) {
			for _, branchInfo := range branchInfos {
				result = append(result, branchInfo.Branch.Name)
			}
			return result
		}

		// Empty batch
		info, err = env.PachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, 0, len(info.Requests))
		require.Equal(t, 0, len(info.Responses))

		// One operation
		info, err = env.PachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
			require.NoError(t, builder.CreateProjectRepo(pfs.DefaultProjectName, "repoA"))
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, 1, len(info.Requests))
		require.Equal(t, 1, len(info.Responses))

		// Two independent operations
		info, err = env.PachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
			require.NoError(t, builder.CreateProjectBranch(pfs.DefaultProjectName, "repoA", "master", "", "", []*pfs.Branch{}))
			require.NoError(t, builder.CreateProjectBranch(pfs.DefaultProjectName, "repoA", "branchA", "master", "", []*pfs.Branch{}))
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, 2, len(info.Requests))
		require.Equal(t, 2, len(info.Responses))

		branchInfos, err = env.PachClient.ListProjectBranch(pfs.DefaultProjectName, "repoA")
		require.NoError(t, err)

		require.ElementsEqual(t, []string{"master", "branchA"}, getBranchNames(branchInfos))

		// Some dependent operations
		info, err = env.PachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
			require.NoError(t, builder.CreateProjectRepo(pfs.DefaultProjectName, "repoB"))
			_, err := builder.StartProjectCommit(pfs.DefaultProjectName, "repoB", "master")
			require.NoError(t, err)
			err = builder.FinishProjectCommit(pfs.DefaultProjectName, "repoB", "master", "")
			require.NoError(t, err)
			require.NoError(t, builder.CreateProjectBranch(pfs.DefaultProjectName, "repoB", "branchA", "master", "", []*pfs.Branch{}))
			require.NoError(t, builder.CreateProjectBranch(pfs.DefaultProjectName, "repoB", "branchB", "branchA", "", []*pfs.Branch{}))
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, 5, len(info.Requests))
		require.Equal(t, 5, len(info.Responses))

		branchInfos, err = env.PachClient.ListProjectBranch(pfs.DefaultProjectName, "repoB")
		require.NoError(t, err)

		require.ElementsEqual(t, []string{"master", "branchA", "branchB"}, getBranchNames(branchInfos))

		for _, branchInfo := range branchInfos {
			if branchInfo.Branch.Name == "master" {
				require.Equal(t, branchInfo.Head, info.Responses[1].Commit)
			}
		}
	})

	suite.Run("TestProjectlessBatch", func(t *testing.T) {
		t.Parallel()
		ctx := pctx.TestContext(t)
		env := realenv.NewRealEnv(ctx, t, dockertestenv.NewTestDBConfig(t))
		_, err := env.PachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
			_, err := builder.PfsAPIClient.CreateRepo(builder.Ctx(), &pfs.CreateRepoRequest{
				Repo: &pfs.Repo{
					Name: "somerepo",
				},
			})
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
	})
}
