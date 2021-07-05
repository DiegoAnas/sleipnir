package at.ac.tuwien.ec.scheduling.offloading.algorithms.group16;


import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.MobileDevice;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Dummy Scheduler that runs all nodes in the mobile device
 */

public class DeviceOnly extends OffloadScheduler {
    /**
     *
     * @param A MobileApplication property from  SimIteration
     * @param I MobileCloudInfrastructure property from  SimIteration
     * Constructors set the parameters and calls setBLevel() to nodes' ranks
     */

	public DeviceOnly(MobileApplication A, MobileCloudInfrastructure I) {
		super();
		setMobileApplication(A);
		setInfrastructure(I);
	}

	public DeviceOnly(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
		super();
		setMobileApplication(t._1());
		setInfrastructure(t._2());
	}

    /**
     * Processor selection phase:
     * select the mobile device
     * @return a (list of) deployment of task components
     */
	@Override
	public ArrayList<? extends OffloadScheduling> findScheduling() {
		double start = System.nanoTime();
		/*scheduledNodes contains the nodes that have been scheduled for execution.
		 * Once nodes are scheduled, they are taken from the PriorityQueue according to their runtime
		 */
		ArrayList<MobileSoftwareComponent> tasks = new ArrayList<>(currentApp.getTaskDependencies().vertexSet());
		ArrayList<OffloadScheduling> deployments = new ArrayList<>();
		//We initialize a new OffloadScheduling object, modelling the scheduling computer with this algorithm
		OffloadScheduling scheduling = new OffloadScheduling();
		//We check until there are nodes available for scheduling
        HashSet<String> outOfBatteryDevices = new HashSet<>();
		for (MobileSoftwareComponent currTask : tasks){
            ComputationalNode userDevice = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
            // deploy it in the mobile device (if enough resources are available)
            // Since capabilities and connectivity shouldn't be an issue, check only if energy budget allows it.
            double consumption = userDevice.getCPUEnergyModel().computeCPUEnergy(currTask, userDevice, currentInfrastructure);
            if (consumption < ((MobileDevice)userDevice).getEnergyBudget()) {
                    if (userDevice.isCompatible(currTask)){
                        deploy(scheduling, currTask, userDevice);
                    } else {
                        userDevice.undeploy(userDevice.getAllocatedTasks().get(0)); // get the first task deployed and undeploy it
                        deploy(scheduling, currTask, userDevice);
                    }
            }else{
                if (!outOfBatteryDevices.contains(userDevice.getId())) { // show the energy message only once (per phone)
                    outOfBatteryDevices.add(userDevice.getId());
                    System.out.println("Mobile energy budget does not allow execution on device");
                }
            }
			/*
			 * if simulation considers mobility, perform post-scheduling operations
			 * (default is to update coordinates of mobile devices)
			 */
			if(OffloadingSetup.mobility)
				postTaskScheduling(scheduling);					
		}
		double end = System.nanoTime();
		scheduling.setExecutionTime(end-start);
		deployments.add(scheduling);
		return deployments;
	}
	
}
