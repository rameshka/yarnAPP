/*
package org.wso2.carbon.das.jobmanager.yarn.deployment;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Logger;
import org.wso2.carbon.das.jobmanager.core.DeploymentManager;
import org.wso2.carbon.das.jobmanager.core.SiddhiAppDeployer;
import org.wso2.carbon.das.jobmanager.core.appCreator.DistributedSiddhiQuery;
import org.wso2.carbon.das.jobmanager.core.appCreator.SiddhiQuery;
import org.wso2.carbon.das.jobmanager.core.deployment.DeploymentManagerImpl;
import org.wso2.carbon.das.jobmanager.core.internal.ServiceDataHolder;
import org.wso2.carbon.das.jobmanager.core.model.ResourceNode;
import org.wso2.carbon.das.jobmanager.core.model.ResourcePool;
import org.wso2.carbon.das.jobmanager.core.model.SiddhiAppHolder;
import org.wso2.carbon.stream.processor.core.distribution.DeploymentStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class YarnDeploymentManagerImpl implements DeploymentManager {
        private static final Logger LOG = Logger.getLogger(DeploymentManagerImpl.class);
        private Iterator resourceIterator;

        @Override
        public DeploymentStatus deploy(DistributedSiddhiQuery distributedSiddhiQuery) {
            Map<String, List<SiddhiAppHolder>> deployedSiddhiAppHoldersMap = ServiceDataHolder
                    .getResourcePool().getSiddhiAppHoldersMap();
            List<SiddhiAppHolder> appsToDeploy = getSiddhiAppHolders(distributedSiddhiQuery);
            List<SiddhiAppHolder> deployedApps = new ArrayList<>();
            boolean shouldDeploy = true;

            if (deployedSiddhiAppHoldersMap.containsKey(distributedSiddhiQuery.getAppName())) {
                List<SiddhiAppHolder> existingApps = deployedSiddhiAppHoldersMap.get(distributedSiddhiQuery.getAppName());
                if (CollectionUtils.isEqualCollection(existingApps, appsToDeploy)) {
                    boolean waitingToDeploy = false;
                    for (SiddhiAppHolder app : existingApps) {
                        if (app.getDeployedNode() == null) {
                            waitingToDeploy = true;
                            break;
                        }
                    }
                    if (waitingToDeploy) {
                        LOG.info(String.format("Exact Siddhi app with name: %s is already exists in waiting mode. " +
                                                       "Hence, trying to re-deploy.", distributedSiddhiQuery.getAppName()));
                        rollback(existingApps);
                    } else {
                        LOG.info(String.format("Exact Siddhi app with name: %s is already deployed.",
                                               distributedSiddhiQuery.getAppName()));
                        shouldDeploy = false;
                    }
                } else {
                    LOG.info("Different Siddhi app with name:" + distributedSiddhiQuery.getAppName() + " is already " +
                                     "deployed. Hence, un-deploying existing Siddhi app.");
                    rollback(deployedSiddhiAppHoldersMap.get(distributedSiddhiQuery.getAppName()));
                }
            }
            boolean isDeployed = true;
            if (shouldDeploy) {
                for (SiddhiAppHolder appHolder : appsToDeploy) {
                    ResourceNode deployedNode = deploy(new SiddhiQuery(appHolder.getAppName(),
                                                                       appHolder.getSiddhiApp()), 0);
                    if (deployedNode != null) {
                        appHolder.setDeployedNode(deployedNode);
                        deployedApps.add(appHolder);
                        LOG.info(String.format("Siddhi app %s of %s successfully deployed in %s.",
                                               appHolder.getAppName(), appHolder.getParentAppName(), deployedNode));
                    } else {
                        LOG.warn(String.format("Insufficient resources to deploy Siddhi app %s of %s. Hence, rolling back.",
                                               appHolder.getAppName(), appHolder.getParentAppName()));
                        isDeployed = false;
                        break;
                    }
                }
                if (isDeployed) {
                    deployedSiddhiAppHoldersMap.put(distributedSiddhiQuery.getAppName(), deployedApps);
                    LOG.info("Siddhi app " + distributedSiddhiQuery.getAppName() + " successfully deployed.");
                } else {
                    rollback(deployedApps);
                    deployedApps = Collections.emptyList();
                    deployedSiddhiAppHoldersMap.remove(distributedSiddhiQuery.getAppName());
                    ServiceDataHolder.getResourcePool().getAppsWaitingForDeploy()
                            .put(distributedSiddhiQuery.getAppName(), appsToDeploy);
                    LOG.info("Siddhi app " + distributedSiddhiQuery.getAppName() + " held back in waiting mode.");
                }
            } else {
                deployedApps = deployedSiddhiAppHoldersMap.get(distributedSiddhiQuery.getAppName());
            }
            ServiceDataHolder.getResourcePool().persist();
            // Returning true as the deployment state, since we might put some apps on wait.
            return getDeploymentStatus(true, deployedApps);
    }

    private List<SiddhiAppHolder> getSiddhiAppHolders(DistributedSiddhiQuery distributedSiddhiQuery) {
        List<SiddhiAppHolder> siddhiAppHolders = new ArrayList<>();
        distributedSiddhiQuery.getQueryGroups().forEach(queryGroup -> {
            queryGroup.getSiddhiQueries().forEach(query -> {
                siddhiAppHolders.add(new SiddhiAppHolder(distributedSiddhiQuery.getAppName(),
                                                         queryGroup.getGroupName(), query.getAppName(), query.getApp(), null));
            });
        });
        return siddhiAppHolders;
    }

    private DeploymentStatus getDeploymentStatus(boolean isDeployed, List<SiddhiAppHolder> siddhiAppHolders) {
        Map<String, List<String>> deploymentDataMap = new HashMap<>();
        for (SiddhiAppHolder appHolder : siddhiAppHolders) {
            if (appHolder.getDeployedNode() != null && appHolder.getDeployedNode().getHttpInterface() != null) {
                if (deploymentDataMap.containsKey(appHolder.getGroupName())) {
                    deploymentDataMap.get(appHolder.getGroupName())
                            .add(appHolder.getDeployedNode().getHttpInterface().getHost());
                } else {
                    List<String> hosts = new ArrayList<>();
                    hosts.add(appHolder.getDeployedNode().getHttpInterface().getHost());
                    deploymentDataMap.put(appHolder.getGroupName(), hosts);
                }
            }
        }
        return new DeploymentStatus(isDeployed, deploymentDataMap);
    }

    */
/**
     * Rollback (un-deploy) already deployed Siddhi apps.
     *
     * @param siddhiAppHolders list of Siddhi app holders to be un deployed.
     *//*

    private void rollback(List<SiddhiAppHolder> siddhiAppHolders) {
        if (siddhiAppHolders != null) {
            siddhiAppHolders.forEach(appHolder -> {
                if (appHolder.getDeployedNode() != null) {
                    if (!SiddhiAppDeployer.unDeploy(appHolder.getDeployedNode(), appHolder.getAppName())) {
                        LOG.warn(String.format("Could not un-deploy Siddhi app %s from %s.",
                                               appHolder.getAppName(), appHolder.getDeployedNode()));
                    } else {
                        appHolder.setDeployedNode(null);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Siddhi app %s un-deployed from %s.",
                                                    appHolder.getAppName(), appHolder.getDeployedNode()));
                        }
                    }
                }
            });
        }
    }

    private ResourceNode deploy(SiddhiQuery siddhiQuery, int retry) {
        ResourcePool resourcePool = ServiceDataHolder.getResourcePool();
        ResourceNode resourceNode = getNextResourceNode();
        ResourceNode deployedNode = null;
        if (resourceNode != null) {
            String appName = SiddhiAppDeployer.deploy(resourceNode, siddhiQuery);
            if (appName == null || appName.isEmpty()) {
                LOG.warn(String.format("Couldn't deploy partial Siddhi app %s in %s", siddhiQuery.getAppName(),
                                       resourceNode));
                if (retry < resourcePool.getResourceNodeMap().size()) {
                    deployedNode = deploy(siddhiQuery, retry + 1);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.warn(String.format("Couldn't deploy partial Siddhi app %s even after %s attempts.",
                                               siddhiQuery.getAppName(), retry));
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Partial Siddhi app %s successfully deployed in %s.",
                                            appName, resourceNode));
                }
                deployedNode = resourceNode;
            }
        }
        return deployedNode;
    }


    public boolean unDeploy(String s) {
        return false;
    }

    public boolean isDeployed(String s) {
        return false;
    }
}
*/
