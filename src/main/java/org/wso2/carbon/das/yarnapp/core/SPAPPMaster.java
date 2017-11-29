package org.wso2.carbon.das.yarnapp.core;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.wso2.carbon.das.jobmanager.core.model.SiddhiAppHolder;
import org.wso2.carbon.das.yarnapp.core.pojo.YarnContainer;
import org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SIDDHIAPP_HOLDER_HDFS_PATH;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SPAPP_MASTER;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SP_DEPLOYER_CLASS;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SP_LOCALIZED_NAME;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SP_PRIORITY_REQUIREMENT;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SP_UNIZIPPED_BUNDLE_NAME;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SP_VCORE;

/**
 * This class is responsible for requesting {@link Container}  from {@link AMRMClientAsync} and Monitoring tasks
 * running on.
 * This class implements requirements of <code>APP Master</code> of <code>Yarn</code> implementation.
 */
public class SPAPPMaster {
    private AMRMClientAsync resourceManager;
    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;
    private Configuration conf;
    private String appMasterHostname;
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";
    private int numContainers;
    private volatile boolean done;
    private AtomicInteger allocContainers = new AtomicInteger();
    private AtomicInteger requestedContainers = new AtomicInteger();
    private AtomicInteger failedContainers = new AtomicInteger();
    private AtomicInteger completedContainers = new AtomicInteger();
    private List<YarnContainer> yarnContainers = new ArrayList<>();
    private List<SiddhiAppHolder> appsToDeploy;
    private List<Thread> launchThreads = new ArrayList<>();
    private static final Logger LOG = Logger.getLogger(SPAPPMaster.class);

    public SPAPPMaster() {

        this.conf = new YarnConfiguration();
    }

    public static void main(String[] args) {
        ContainerId containerId =
                ConverterUtils.toContainerId(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));

        SPAPPMaster spappMaster = new SPAPPMaster();
        try {
            spappMaster.init();
            spappMaster.run();
        } catch (IOException e) {
            LOG.error(e);
        } catch (YarnException e) {
            LOG.error(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Moving dis-integrated siddhi-Files in to HDFS
     *
     * @throws IOException
     */
    private void init() throws IOException, ClassNotFoundException {
        conf.addResource(new Path("file:///usr/local/hadoop/etc/hadoop/core-site.xml")); // Replace with actual path
        conf.addResource(new Path("file:///usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        appsToDeploy = deserializeSiddhiAppHolders();
        numContainers = findContainerRequirement(appsToDeploy);
        writeToHDFS(appsToDeploy);
    }

    private int findContainerRequirement(List<SiddhiAppHolder> siddhiAppHolders) {
        Map<String, Integer> execGroupParallelismMap = new HashMap<>();
        for (SiddhiAppHolder siddhiAppHolder : siddhiAppHolders) {
            String execGroupName = siddhiAppHolder.getGroupName();
            if (execGroupParallelismMap.containsKey(execGroupName)) {
                int count = execGroupParallelismMap.get(execGroupName);
                execGroupParallelismMap.put(execGroupName, count + 1);
            } else {
                execGroupParallelismMap.put(siddhiAppHolder.getGroupName(), 1);
            }
        }
        return Collections.max(execGroupParallelismMap.values());
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("Starting SPAPPMaster.....");
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        resourceManager.init(conf);
        resourceManager.start();


        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse applicationMasterResponse =
                resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        int maxMemoryCluster = applicationMasterResponse.getMaximumResourceCapability().getMemory();
        int virtualCores = applicationMasterResponse.getMaximumResourceCapability().getVirtualCores();

        LOG.info("Max memory capability of the cluster: Memory<" + maxMemoryCluster
                         + "> ,Max Vcore capability of the cluster: Vcores <"
                         + virtualCores
                         + ">");


        int requiredContainerMemory = SPAPPMasterConstants.CONTAINER_MEMORY * numContainers;

        if (requiredContainerMemory < maxMemoryCluster) {
            //if required memory is much greater than available then can wait till cluster get released - wait for
            // a certain time interval, but if the specified memory for yarn cluster is lower than the required then
            // waiting for freed memory can not be done.
            LOG.info("Max memory capability of the cluster  and Assigned mismatch...\n");
            // TODO: 11/26/17 throw exception to for Illegal resource request.
        }

        LOG.info("Requesting Containers from Resource Manager");

        for (int i = 0; i < numContainers; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskFromRM();
            resourceManager.addContainerRequest(containerAsk);
        }

        requestedContainers.set(numContainers);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                LOG.error("SPAPPMaster Interrupted");
            }
        }

        finish();
        return true;
    }

    private void finish() {
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();
        LOG.info("Application completed.Unregistering SPAPPMaster from ResourceManager");
        try {
            resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
            LOG.info("SPAPPMaster Successfully Unregistered ");
        } catch (YarnException e) {
            LOG.error("SPAPPMaster Unregistration failure", e);
        } catch (IOException e) {
            LOG.error("SPAPPMaster Unregistration failure", e);
        }
        done = true;
        LOG.info("Application completed. Stopping ResourceManager Instance");
        resourceManager.stop();
    }

    private AMRMClient.ContainerRequest setupContainerAskFromRM() {
        //TODO:resource requirements  depend on the user
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(SP_PRIORITY_REQUIREMENT);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(SPAPPMasterConstants.CONTAINER_MEMORY);
        capability.setVirtualCores(SP_VCORE);
        //resources are not requested implicitly depending on a specific node or rack in the cluster.
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, pri);
        return request;
    }

    private void writeToHDFS(List<SiddhiAppHolder> appsToDeploy) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        for (SiddhiAppHolder siddhiAppHolder : appsToDeploy) {
            String appPath = fs.getHomeDirectory() + File.separator + siddhiAppHolder.getParentAppName() + File
                    .separator + siddhiAppHolder.getAppName() + ".ser";
            Path hdfsPath = new Path(appPath);
            ObjectOutputStream oos = new ObjectOutputStream(fs.create(hdfsPath));
            oos.writeObject(siddhiAppHolder);
            oos.close();
        }
    }

    /**
     * Locate serialized {@link SiddhiAppHolder} and reforming the corresponding instance
     */
    private List<SiddhiAppHolder> deserializeSiddhiAppHolders() throws IOException, ClassNotFoundException {
        // TODO: 11/15/17 find localized file path
        FileSystem fs = FileSystem.get(conf);
        String hdfsPath = fs.getHomeDirectory().toUri() + File.separator + SIDDHIAPP_HOLDER_HDFS_PATH;
        InputStream in = fs.open(new Path(hdfsPath));

        ObjectInputStream objReader = new ObjectInputStream(in);
        List<SiddhiAppHolder> siddhiAppHolderList =( List<SiddhiAppHolder>) objReader.readObject();
        objReader.close();

        return siddhiAppHolderList;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            int exitStatus;
            for (ContainerStatus sts : statuses) {
                exitStatus = sts.getExitStatus();
                if (exitStatus == 0) {
                    LOG.info("Successfully completed container ID: " + sts.getContainerId());
                } else {
                    if (ContainerExitStatus.ABORTED == exitStatus) {
                        //need to reschedule the container again
                        requestedContainers.decrementAndGet();
                        allocContainers.decrementAndGet();
                        LOG.info("Container killed by the framework: " + sts.getContainerId()
                                         + "..new  container will be rescheduled");
                    } else {
                        //container being killed due to different reason --->here not allocating them again
                        completedContainers.incrementAndGet();
                        failedContainers.incrementAndGet();
                        //get diagnostic message of failed containers
                        LOG.info(sts.getContainerId() + " Container terminated , new container will not be rescheduled:"
                                         + "due to " + sts.getDiagnostics());
                    }
                }
            }
            int reschedule = numContainers - requestedContainers.get();
            if (reschedule > 0) {
                for (int i = 0; i < reschedule; ++i) {
                    AMRMClient.ContainerRequest containerAsk = setupContainerAskFromRM();
                    resourceManager.addContainerRequest(containerAsk);
                }
            } else {
                done = true;
            }
        }

        /**
         * Distribution of SiddhiApps among available Resources starts here.
         *
         * @param allocatedContainers
         */
        public void onContainersAllocated(List<Container> allocatedContainers) {
            for (Container allocatedContainer : allocatedContainers) {
                yarnContainers.add(new YarnContainer(new HashMap<>(), allocatedContainer));
            }
            if (yarnContainers.size() == numContainers) {  //this value depending on the # of containers for the
                //siddhiAPPS are added to the containers in a normal-distribution method
                //the implementation will guarantee only that there will be only single siddhiApp from same execGroup
                //residing in a SP-instance
                for (SiddhiAppHolder siddhiAppHolder : appsToDeploy) {
                    String execGroupName = siddhiAppHolder.getGroupName();
                    for (YarnContainer yarnContainer : yarnContainers) {
                        if (yarnContainer.getSiddhiAppList().get(execGroupName) != null) {
                            continue;
                        } else {
                            yarnContainer.setParentAPPName(siddhiAppHolder.getParentAppName());
                            yarnContainer.addSiddhiAPPName(execGroupName, siddhiAppHolder.getAppName());
                            break;
                        }

                    }
                }
                for (YarnContainer yarnContainer : yarnContainers) {
                    LaunchContainerRunnable runnableLaunchContainer =
                            new LaunchContainerRunnable(yarnContainer, containerListener);
                    Thread launchThread = new Thread(runnableLaunchContainer);
                    launchThreads.add(launchThread);
                    launchThread.start();
                }
            }
        }

        public void onShutdownRequest() {
            LOG.info("Shutting down SiddhiMaster on ResourceManager request");
            done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        public float getProgress() {
            return 0;
        }

        public void onError(Throwable e) {
            LOG.error("ResourceManager communication error.Stopping the RM instance:", e);
            resourceManager.stop();
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

        }

        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        public void onContainerStopped(ContainerId containerId) {

        }

        public void onStartContainerError(ContainerId containerId, Throwable t) {

        }

        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

        }

        public void onStopContainerError(ContainerId containerId, Throwable t) {

        }
    }

    private class LaunchContainerRunnable implements Runnable {

        NMCallbackHandler containerListener;
        Container container;
        List<String> siddhiAPPNameList;
        String parentAPPName;

        public LaunchContainerRunnable(YarnContainer yarnContainer, NMCallbackHandler containerListener) {
            this.container = yarnContainer.getContainer();
            this.containerListener = containerListener;
            this.siddhiAPPNameList = new ArrayList<>(yarnContainer.getSiddhiAppList().values());
            this.parentAPPName = yarnContainer.getParentAPPName();
        }

        public void run() {
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            String classpath = "$CLASSPATH:./" + SP_LOCALIZED_NAME;
            Map<String, String> env = new HashMap<>();
            env.put("CLASSPATH", classpath);
            ctx.setEnvironment(env);
            Map<String, LocalResource> localResources = new HashMap<>();

            String applicationId = container.getId().getApplicationAttemptId().getApplicationId().toString();
            try {
                FileSystem fs = FileSystem.get(conf);
                //asuming that user has put the file to HDFS
                Path workerDestination = new Path(fs.getHomeDirectory()
                                                          + File.separator
                                                          + SPAPPMasterConstants.SP_HDFS_NAME);
                FileStatus destStatus = fs.getFileStatus(workerDestination);
                LocalResource workerRsrc = Records.newRecord(LocalResource.class);
                workerRsrc.setType(LocalResourceType.FILE);
                workerRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
                workerRsrc.setResource(ConverterUtils.getYarnUrlFromPath(workerDestination));
                workerRsrc.setTimestamp(destStatus.getModificationTime());
                workerRsrc.setSize(destStatus.getLen());
                localResources.put(SP_LOCALIZED_NAME, workerRsrc);


                Path siddhiAPPFile = new Path(fs.getHomeDirectory()
                                                      + File.separator
                                                      + SPAPP_MASTER);

                FileStatus siddhiAPPFileDist = fs.getFileStatus(siddhiAPPFile);
                LocalResource siddhiAPPFileRsrc = Records.newRecord(LocalResource.class);
                siddhiAPPFileRsrc.setType(LocalResourceType.FILE);
                siddhiAPPFileRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
                siddhiAPPFileRsrc.setResource(ConverterUtils.getYarnUrlFromPath(siddhiAPPFile));
                siddhiAPPFileRsrc.setTimestamp(siddhiAPPFileDist.getModificationTime());
                siddhiAPPFileRsrc.setSize(siddhiAPPFileDist.getLen());
                localResources.put(SPAPP_MASTER, siddhiAPPFileRsrc);
                //   }

            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            ctx.setLocalResources(localResources);

            String containerHome = conf.get("yarn.nodemanager.local-dirs")
                    + File.separator + ContainerLocalizer.USERCACHE
                    + File.separator
                    + System.getenv().get(ApplicationConstants.Environment.USER.toString())
                    + File.separator + ContainerLocalizer.APPCACHE
                    + File.separator + applicationId + File.separator
                    + container.getId().toString();

            List<String> commands = new ArrayList<>();

            StringBuilder classPathEnv = new StringBuilder("");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(c.trim());
                classPathEnv.append(File.pathSeparatorChar);
            }


            String containerLaunchSiddhiAppNameList = containerLaunchAppList(siddhiAPPNameList);

            commands.add(" tar zxvf " + SP_LOCALIZED_NAME + " -C ./ ");
            commands.add(" && ");

            commands.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java -cp "
                                 + containerHome
                                 + File.separator
                                 + SPAPP_MASTER
                                 + File.pathSeparator
                                 + classPathEnv.toString()
                                 + " "
                                 + SP_DEPLOYER_CLASS
                                 + " "
                                 + containerHome
                                 + File.separator
                                 + SP_UNIZIPPED_BUNDLE_NAME
                                 + " "
                                 + parentAPPName
                                 + " "
                                 + containerLaunchSiddhiAppNameList
                                 + " && "
                                  +  String.format("%s%s%s%sbin%sworker.sh",containerHome,File.separator,
                                                 SP_UNIZIPPED_BUNDLE_NAME, File.separator, File.separator)
                                 + " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                                 + " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr ");

            ctx.setCommands(commands);
            nmClientAsync.startContainerAsync(container, ctx);


        }

    }

    private String containerLaunchAppList(List<String> appNameList) {
        StringBuilder stringBuilder = new StringBuilder(" ");
        for (String s : appNameList) {
            LOG.info(s);
            stringBuilder.append(s).append(" ");
        }
        return stringBuilder.toString();
    }

}