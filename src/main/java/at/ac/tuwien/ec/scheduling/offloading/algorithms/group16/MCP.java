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

public class MCP extends OffloadScheduler {
    private Map<Executable, Integer> ALAPTimes = new HashMap<>();
    private Map<Executable, List<Integer>> ALAPLists = new HashMap<>();
    private int ALAPMin = Integer.MAX_VALUE;

    @Override
    public ArrayList<? extends OffloadScheduling> findScheduling() {
        PriorityQueue<MobileSoftwareComponent> tasks = new PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());
	//To start, we add all nodes in the workflow
	tasks.addAll(currentApp.getTaskDependencies().vertexSet());
	ArrayList<OffloadScheduling> deployments = new ArrayList<OffloadScheduling>();

        List<ComponentLink> readyAsList = Arrays.asList(tasks);
        List<ComponentLink> sorted = alapLists.entrySet().stream()
                .filter(entry -> readyAsList.contains(entry.getKey()))
                .sorted((x, y) ->
                        x.getValue().toString().compareTo(y.getValue().toString()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        return sorted.get(0);
    }


    @Override
    public boolean usesPriority() {
        return true;
    }

    @Override
    public void calculatePriorities(OffloadScheduling schedule) {
        calculateALAP(schedule.getDependencies());
        schedule.getDependencies().vertexSet().forEach(exe -> ALAPAllNodes(schedule.getDependencies(), exe));
    }

    private void calculateALAP(DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag) {
        Set<ComponentLink> allVertices = dag.vertexSet();
        allVertices.stream()
                .filter(task -> dag.inDegreeOf(task) == 0)
                .forEach(task -> ALAPNode(dag, task));
        int executionTime = -this.ALAPMin;
        allVertices.forEach(exe -> this.ALAPTimes.put(exe, executionTime + this.ALAPTimes.get(exe)));
    }

    protected int ALAPNode(final DirectedDirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag, MobileSoftwareComponent cur) {
        if (this.ALAPTimes.containsKey(cur)) {
            return this.ALAPTimes.get(cur);
        }

        if (dag.outDegreeOf(cur) == 0) {
            int tmp = -cur.getExecutionTime();
            this.ALAPTimes.put(cur, tmp);
            this.ALAPMin = Math.min(tmp, this.ALAPMin);
            return tmp;
        } else {
            Set<DefaultEdge> edges = dag.outgoingEdgesOf(cur);
            Set<ComponentLink> neighs = edges.stream().map(dag::getEdgeTarget);
            int minStartTimeOfNeighbours = neighs.map(next -> ALAPNode(dag, next)).min(Integer::compare).get();
            int minStartTimeOfCurrent = -cur.getExecutionTime() + minStartTimeOfNeighbours;
            this.ALAPMin = Math.min(minStartTimeOfCurrent, this.ALAPMin);
            this.ALAPTimes.put(cur, minStartTimeOfCurrent);
            return minStartTimeOfCurrent;
        }

    }

    protected List<Integer> ALAPAllNodes(DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag, MobileSoftwareComponent cur) {
        //calculate ALAP list for current node
        if (alapLists.containsKey(cur)) {
            return alapLists.get(cur);
        }

        if (dag.outDegreeOf(cur) == 0) {
            //if number of outgoing edges is zero -> leaf
            List<Integer> res = Collections.singletonList(this.ALAPTimes.get(cur));
            alapLists.put(cur, res);
            return res;
        } else {
            Set<DefaultEdge> edges = dag.outgoingEdgesOf(cur);
            Set<ComponentLink> neighs = edges.stream().map(dag::getEdgeTarget);
            List<Integer> res = new LinkedList<>();
            neighs.map(next -> ALAPAllNodes(dag, next))
                    .collect(Collectors.toList())
                    .forEach(res::addAll);
            res.add(this.ALAPTimes.get(cur));
            res.sort(Integer::compare);
            alapLists.put(cur, res);
            return res;
        }
    }
	
}
