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
import org.apache.commons.collections.SetUtils;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import scala.Tuple2;

import java.util.*;

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

public class DynLevelSched extends OffloadScheduler {
    /**
     *
     * @param A MobileApplication property from  SimIteration
     * @param I MobileCloudInfrastructure property from  SimIteration
     * Constructors set the parameters and calls setBLevel() to nodes' ranks
     */

	public DynLevelSched(MobileApplication A, MobileCloudInfrastructure I) {
		super();
		setMobileApplication(A);
		setInfrastructure(I);
		setBLevel(this.currentApp,this.currentInfrastructure);
	}

	public DynLevelSched(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
		super();
		setMobileApplication(t._1());
		setInfrastructure(t._2());
		setBLevel(this.currentApp,this.currentInfrastructure);
	}

    /**
     * Processor selection phase:
     * every iteration the Dynamic Level gets calculated
     * which is the static bLevel - EarliesStartTime
     * @return
     */
    @Override
    public ArrayList<? extends OffloadScheduling> findScheduling() {
        double start = System.nanoTime();
        //We initialize a new OffloadScheduling object, modelling the scheduling computer with this algorithm
        OffloadScheduling scheduling = new OffloadScheduling();
        //We check until there are nodes available for scheduling
        /*
         * tasks contains tasks that have to be scheduled for execution.
         * Tasks are selected according to their static b-level and EST (for DLS)
         */
        HashMap<String, MobileSoftwareComponent> readyTasks = new HashMap<>();
        HashSet<MobileSoftwareComponent> scheduledTasks = new HashSet<>();
        DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag = currentApp.getTaskDependencies();
        TopologicalOrderIterator taskIterator = new TopologicalOrderIterator(dag);
        System.out.println(" Initial number of tasks :"+dag.vertexSet().size());
        while (taskIterator.hasNext()){
            // add the source nodes to the readyList
            MobileSoftwareComponent node =(MobileSoftwareComponent) taskIterator.next();
            if (dag.getAncestors(node).isEmpty()) //only add source nodes at first
                readyTasks.put(node.getId(), node);
            else
                break; // since list is traversed topologically, once the first child is visited there are no more source nodes
        }
        int taskCounter = 0;
        HashSet<MobileSoftwareComponent> auxSet = new HashSet<>();
        MobileSoftwareComponent maxTask;
        ComputationalNode maxCN;
        Double maxDL;
        Double currDL = 0.0;
        System.out.println(" Initial number of ready tasks :"+readyTasks.size());
        while (!readyTasks.isEmpty()){
            maxTask =null;
            maxCN = null;
            maxDL = -Double.MAX_VALUE;
            for (MobileSoftwareComponent currTask : readyTasks.values()){
                // look for the task-processor pair with the maximum Dynamic Level
                if (currTask.isOffloadable()){
                    for(ComputationalNode cn : currentInfrastructure.getAllNodes()){
                        currDL = cn.getESTforTask(currTask) - currTask.getRank();
                        if( currDL > maxDL){
                            maxTask = currTask;
                            maxDL = currDL;
                            maxCN = cn;
                        }
                    }
                }
                currDL = ((ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId())).getESTforTask(currTask) - currTask.getRank();
                if( currDL > maxDL){
                    maxTask = currTask;
                    maxDL = currDL;
                    maxCN = ((ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId()));
                }
            }
            if (maxTask== null){
                System.out.print("Null task. Last DL calculate: "+ currDL);
            } else {
                deploy(scheduling, maxTask, maxCN);
                for (ComponentLink task : dag.outgoingEdgesOf(maxTask)){
                    if (!readyTasks.containsKey(task.getTarget().getId()))
                        readyTasks.put(task.getTarget().getId(), task.getTarget());
                }
                readyTasks.remove(maxTask.getId());
                if (scheduledTasks.contains(maxTask.getId()))
                    System.out.println("Task duplicated :"+maxTask.getId());
                else
                    scheduledTasks.add(maxTask);
                    System.out.println("Task deployed :"+ maxTask.getId());
                System.out.println("Tasks deployed :"+taskCounter);
                taskCounter +=1;
            }
            System.out.println("Tasks remaining in ready list :"+readyTasks.size());
            auxSet.addAll(dag.vertexSet());
            auxSet.removeAll(scheduledTasks);
            int unscheduled = auxSet.size();
            auxSet.clear();
            System.out.println("Tasks unscheduled : "+unscheduled);
            /*
             * if simulation considers mobility, perform post-scheduling operations
             * (default is to update coordinates of mobile devices)
             */
            if(OffloadingSetup.mobility)
                postTaskScheduling(scheduling);
        }

        double end = System.nanoTime();
        scheduling.setExecutionTime(end-start);
        return new ArrayList<OffloadScheduling>() {{add(scheduling);}};
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
