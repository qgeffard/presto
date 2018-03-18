/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestQueues
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManager()
            throws Exception
    {
        testBasic(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManager()
            throws Exception
    {
        testBasic(true);
    }

    private void testBasic(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            // submit first "dashboard" query
            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);

            // wait for the first "dashboard" query to start
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            // submit second "dashboard" query
            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

            // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            // submit first non "dashboard" query
            QueryId firstNonDashboardQuery = createAdHocQuery(queryRunner);

            // wait for the first non "dashboard" query to start
            waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);

            // submit second non "dashboard" query
            QueryId secondNonDashboardQuery = createAdHocQuery(queryRunner);

            // wait for the second non "dashboard" query to start
            waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);

            // cancel first "dashboard" query, second "dashboard" query and second non "dashboard" query should start running
            cancelQuery(queryRunner, firstDashboardQuery);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
        }
    }

    @Test(timeOut = 240_000)
    public void testExceedSoftLimits()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("experimental.resource-groups-enabled", "true"))) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_soft_limits.json")));

            QueryId scheduled1 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled1, RUNNING);

            QueryId scheduled2 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled2, RUNNING);

            QueryId scheduled3 = createScheduledQuery(queryRunner);
            waitForQueryState(queryRunner, scheduled3, RUNNING);

            // cluster is now 'at capacity' - scheduled is running 3 (i.e. over soft limit)

            QueryId backfill1 = createBackfill(queryRunner);
            QueryId scheduled4 = createScheduledQuery(queryRunner);

            cancelQuery(queryRunner, scheduled1);

            // backfill should be chosen to run next
            waitForQueryState(queryRunner, backfill1, RUNNING);

            cancelQuery(queryRunner, scheduled2);
            cancelQuery(queryRunner, scheduled3);
            cancelQuery(queryRunner, scheduled4);

            QueryId backfill2 = createBackfill(queryRunner);
            waitForQueryState(queryRunner, backfill2, RUNNING);

            QueryId backfill3 = createBackfill(queryRunner);
            waitForQueryState(queryRunner, backfill3, RUNNING);

            // cluster is now 'at capacity' - backfills is running 3 (i.e. over soft limit)

            QueryId backfill4 = createBackfill(queryRunner);
            QueryId scheduled5 = createScheduledQuery(queryRunner);
            cancelQuery(queryRunner, backfill1);

            // scheduled should be chosen to run next
            waitForQueryState(queryRunner, scheduled5, RUNNING);
            cancelQuery(queryRunner, backfill2);
            cancelQuery(queryRunner, backfill3);
            cancelQuery(queryRunner, backfill4);
            cancelQuery(queryRunner, scheduled5);

            waitForQueryState(queryRunner, scheduled5, FAILED);
        }
    }

    private QueryId createBackfill(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("backfill", ImmutableSet.of()), LONG_LASTING_QUERY);
    }

    private QueryId createScheduledQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("scheduled", ImmutableSet.of()), LONG_LASTING_QUERY);
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        testTwoQueriesAtSameTime(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        testTwoQueriesAtSameTime(true);
    }

    private void testTwoQueriesAtSameTime(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);

            ImmutableSet<QueryState> queuedOrRunning = ImmutableSet.of(QUEUED, RUNNING);
            waitForQueryState(queryRunner, firstDashboardQuery, queuedOrRunning);
            waitForQueryState(queryRunner, secondDashboardQuery, queuedOrRunning);
        }
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        testTooManyQueries(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        testTooManyQueries(true);
    }

    private void testTooManyQueries(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            QueryId secondDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            QueryId thirdDashboardQuery = createDashboardQuery(queryRunner);
            waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);
        }
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerRejection()
            throws Exception
    {
        testRejection(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerRejection()
            throws Exception
    {
        testRejection(true);
    }

    @Test(timeOut = 240_000)
    public void testClientTagsBasedSelection()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of("experimental.resource-groups-enabled", "true"))) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_client_tags_based_config.json")));
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a")), LONG_LASTING_QUERY, "global.a.default");
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("b")), LONG_LASTING_QUERY, "global.b");
            assertResourceGroup(queryRunner, newSessionWithTags(ImmutableSet.of("a", "c")), LONG_LASTING_QUERY, "global.a.c");
        }
    }

    @Test(timeOut = 240_000)
    public void testQueryTypeBasedSelection()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("experimental.resource-groups-enabled", "true")
                .build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_query_type_based_config.json")));
            assertResourceGroup(queryRunner, newAdhocSession(), LONG_LASTING_QUERY, "global.select");
            assertResourceGroup(queryRunner, newAdhocSession(), "SHOW TABLES", "global.describe");
            assertResourceGroup(queryRunner, newAdhocSession(), "EXPLAIN " + LONG_LASTING_QUERY, "global.explain");
            assertResourceGroup(queryRunner, newAdhocSession(), "DESCRIBE lineitem", "global.describe");
            assertResourceGroup(queryRunner, newAdhocSession(), "RESET SESSION " + HASH_PARTITION_COUNT, "global.data_definition");
        }
    }

    private void assertResourceGroup(DistributedQueryRunner queryRunner, Session session, String query, String expectedResourceGroup)
            throws InterruptedException
    {
        QueryId queryId = createQuery(queryRunner, session, query);
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(RUNNING, FINISHED));
        Optional<String> resourceGroupName = queryRunner.getCoordinator().getQueryManager().getQueryInfo(queryId).getResourceGroupName();
        assertTrue(resourceGroupName.isPresent(), "Query should have a resource group");
        assertEquals(resourceGroupName.get().toString(), expectedResourceGroup, format("Expected: '%s' resource group, found: %s", expectedResourceGroup, resourceGroupName.get()));
    }

    private void testRejection(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            assertEquals(queryManager.getQueryInfo(queryId).getErrorCode(), QUERY_REJECTED.toErrorCode());
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private QueryId createDashboardQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newSession("dashboard", ImmutableSet.of()), LONG_LASTING_QUERY);
    }

    private QueryId createAdHocQuery(DistributedQueryRunner queryRunner)
    {
        return createQuery(queryRunner, newAdhocSession(), LONG_LASTING_QUERY);
    }

    private static Session newAdhocSession()
    {
        return newSession("adhoc", ImmutableSet.of());
    }

    private static Session newRejectionSession()
    {
        return newSession("reject", ImmutableSet.of());
    }

    private static Session newSessionWithTags(Set<String> clientTags)
    {
        return newSession("sessionWithTags", clientTags);
    }

    private static Session newSession(String source, Set<String> clientTags)
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource(source)
                .setClientTags(clientTags)
                .build();
    }
}
