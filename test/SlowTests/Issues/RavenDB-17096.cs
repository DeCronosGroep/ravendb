﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17096 : ReplicationTestBase
    {
        public RavenDB_17096(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EtlFromReAddedNodeShouldWork()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var src = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            using (var dest = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var connectionStringName = "EtlFailover";
                var urls = nodes.Select(n => n.WebUrl).ToArray();
                var config = new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    LoadRequestTimeoutInSec = 10,
                    MentorNode = "A",
                    Transforms = new List<Transformation>
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            ApplyToAllDocuments = true,
                            IsEmptyScript = true
                        }
                    }
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                var result = await src.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                await src.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(config));
                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Store(new User()
                    {
                        Name = "John Dire"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "John Dire", 30_000));

                var deletion = await src.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(src.Database, hardDelete: true, fromNode: "A",
                    timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                await WaitForRaftIndexToBeAppliedInCluster(deletion.RaftCommandIndex, TimeSpan.FromSeconds(30));

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);

                    session.Store(new User()
                    {
                        Name = "John Doe"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "John Doe", 30_000));

                var addResult = await src.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(src.Database, node: "A"));
                await WaitForRaftIndexToBeAppliedInCluster(addResult.RaftCommandIndex, TimeSpan.FromSeconds(30));

                await WaitAndAssertForValueAsync(() => GetMembersCount(src), 3);

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);

                    session.Store(new User()
                    {
                        Name = "John Doe"
                    }, "marker");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "marker", u => u.Name == "John Doe", 30_000));
            }
        }
    }
}
