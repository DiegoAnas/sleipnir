package at.ac.tuwien.ec.scheduling.offloading.algorithms.group16;


import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.heftbased.utils.NodeRankComparator;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import org.jgrapht.graph.DirectedAcyclicGraph;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.PriorityQueue;

/**
 * OffloadScheduler class that implements the
 * Highest Level First with Estimated Times (HLFET) algorithm
 * , a simple scheduling heuristic with O(v^2) time-complexity
 *
 * As described in:
 * Kwok, Y., & Ahmad, I. (1999).
 * Static scheduling algorithms for allocating directed task graphs to multiprocessors.
 * ACM Comput. Surv., 31, 406-471.
 */

public class HLFET extends OffloadScheduler {
    /**
     *
     * @param A MobileApplication property from  SimIteration
     * @param I MobileCloudInfrastructure property from  SimIteration
     * Constructors set the parameters and calls setBLevel() to nodes' ranks
     */

	public HLFET(MobileApplication A, MobileCloudInfrastructure I) {
		super();
		setMobileApplication(A);
		setInfrastructure(I);
		setBLevel(this.currentApp,this.currentInfrastructure);
	}

	public HLFET(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
		super();
		setMobileApplication(t._1());
		setInfrastructure(t._2());
		setBLevel(this.currentApp,this.currentInfrastructure);
	}

    /**
     * Processor selection phase:
     * select the tasks in order of their priorities and schedule them on its "best" processor,
     * which minimizes task's finish time
     * @return
     */
	@Override
	public ArrayList<? extends OffloadScheduling> findScheduling() {
		double start = System.nanoTime();
		/*scheduledNodes contains the nodes that have been scheduled for execution.
		 * Once nodes are scheduled, they are taken from the PriorityQueue according to their runtime
		 */
		PriorityQueue<MobileSoftwareComponent> scheduledNodes 
		= new PriorityQueue<MobileSoftwareComponent>(new RuntimeComparator());
		/*
		 * tasks contains tasks that have to be scheduled for execution.
		 * Tasks are selected according to their staticBLevel (at least in HEFT)
		 */
		PriorityQueue<MobileSoftwareComponent> tasks = new PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());
		//To start, we add all nodes in the workflow
		tasks.addAll(currentApp.getTaskDependencies().vertexSet());
		ArrayList<OffloadScheduling> deployments = new ArrayList<OffloadScheduling>();
				
		MobileSoftwareComponent currTask;
		//We initialize a new OffloadScheduling object, modelling the scheduling computer with this algorithm
		OffloadScheduling scheduling = new OffloadScheduling(); 
		//We check until there are nodes available for scheduling
		while((currTask = tasks.poll())!=null)
		{
			//If there are nodes to be scheduled, we check the first task who terminates and free its resources
			if(!scheduledNodes.isEmpty())
			{
				MobileSoftwareComponent firstTaskToTerminate = scheduledNodes.remove();
				((ComputationalNode) scheduling.get(firstTaskToTerminate)).undeploy(firstTaskToTerminate);
			}
			double tMin = Double.MAX_VALUE; //Minimum execution time for next task
			ComputationalNode target = null;
			if(!currTask.isOffloadable())
			{
			    // If task is not offloadable, deploy it in the mobile device (if enough resources are available)
                if(isValid(scheduling,currTask,(ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId())))
                	target = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId()); 
				
			}
			else
			{
				for(ComputationalNode cn : currentInfrastructure.getAllNodes())
					if(currTask.getRuntimeOnNode(cn, currentInfrastructure) < tMin &&
							isValid(scheduling,currTask,cn))
					{
						tMin = currTask.getRuntimeOnNode(cn, currentInfrastructure); // Earliest Finish Time  EFT = wij + EST
						target = cn;
					}
				
			}
			//if scheduling found a target node for the task, it allocates it to the target node
			if(target != null)
			{
				deploy(scheduling,currTask,target);
				scheduledNodes.add(currTask);
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

	protected void setBLevel(MobileApplication A, MobileCloudInfrastructure I)
	{
		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
			msc.setVisited(false);
				
		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())		
			staticBLevel(msc,A.getTaskDependencies(),I);

	}

    /**
     * staticBLevel is the task prioritizing phase of HLFET
     * it is computed recuversively by traversing the task graph upward
     * instead of having a topological ordered list as defined.
     * For computing the b-level:
     * The execution cost is equal to the Millions of Instructions of the node
     * The communication cost is equal to the amount of input data the successor requires
     * @param msc
     * @param dag Mobile Application's DAG
     * @param infrastructure
     * @return node b-level
     */
	private double staticBLevel(MobileSoftwareComponent msc, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
                                MobileCloudInfrastructure infrastructure) {
		double execCost = 0.0; // Execution task cost, measured in millionsOfInstructions
		if(!msc.isVisited())
        /*  since is defined recursively, visited makes sure no extra unnecessary computations are done when
		    calling staticBLevel on all nodes during initialization */
        {
			msc.setVisited(true);
			execCost = msc.getMillionsOfInstruction();

            double neighBLevel;
            double maxnNeighBLevel = 0; // max neighbour blevel
            for(ComponentLink neigh : dag.outgoingEdgesOf(msc))
            {
                // blevel = execCost +  max(cij + blevel(j))    for all j in succ(i)
                // where cij is the commmunication cost between nodes
                neighBLevel = staticBLevel(neigh.getTarget(),dag,infrastructure); // succesor's rank
                double commCost = 0;  // this component's Communication cost to its successors, measured by the inData of the successor
                if(neigh.getTarget().isOffloadable())
                {
                    commCost = neigh.getTarget().getInData();
                }
                double auxBLevel = neighBLevel + commCost;
                maxnNeighBLevel = (auxBLevel > maxnNeighBLevel)? auxBLevel : maxnNeighBLevel;
            }
            msc.setRank(execCost + maxnNeighBLevel);
		}
		return msc.getRank();
	}
	
}
