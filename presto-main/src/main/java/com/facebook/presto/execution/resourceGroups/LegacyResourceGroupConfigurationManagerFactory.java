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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class LegacyResourceGroupConfigurationManagerFactory
        implements ResourceGroupConfigurationManagerFactory
{
    public static final String LEGACY_RESOURCE_GROUP_MANAGER = "legacy";
    private static final ResourceGroupId GLOBAL = new ResourceGroupId("global");

    private final int hardConcurrencyLimit;
    private final int maxQueued;

    @Inject
    public LegacyResourceGroupConfigurationManagerFactory(QueryManagerConfig config)
    {
        hardConcurrencyLimit = config.getMaxConcurrentQueries();
        maxQueued = config.getMaxQueuedQueries();
    }

    @Override
    public String getName()
    {
        return LEGACY_RESOURCE_GROUP_MANAGER;
    }

    @Override
    public ResourceGroupConfigurationManager<VoidContext> create(Map<String, String> config, ResourceGroupConfigurationManagerContext context)
    {
        return new LegacyResourceGroupConfigurationManager();
    }

    public class LegacyResourceGroupConfigurationManager
            implements ResourceGroupConfigurationManager<VoidContext>
    {
        @Override
        public void configure(ResourceGroup group, SelectionContext<VoidContext> criteria)
        {
            checkArgument(group.getId().equals(GLOBAL), "Unexpected resource group: %s", group.getId());
            group.setMaxQueuedQueries(maxQueued);
            group.setHardConcurrencyLimit(hardConcurrencyLimit);
        }

        @Override
        public Optional<SelectionContext<VoidContext>> match(SelectionCriteria criteria)
        {
            return Optional.of(new SelectionContext<>(GLOBAL, VoidContext.NONE));
        }
    }

    private enum VoidContext { NONE }
}
