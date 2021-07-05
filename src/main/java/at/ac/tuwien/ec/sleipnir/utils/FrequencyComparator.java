package at.ac.tuwien.ec.sleipnir.utils;

import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import scala.Tuple2;
import scala.Tuple5;

import java.io.Serializable;
import java.util.Comparator;

/* Class used to compare deployments according to frequency, i.e., the number of times they appear in the histogram
 */
public class FrequencyComparator implements Serializable,
        Comparator<Tuple2<OffloadScheduling, Tuple5<Integer, Double, Double, Double, Double>>>
{

    private static final long serialVersionUID = -2034500309733677393L;

    public int compare(Tuple2<OffloadScheduling, Tuple5<Integer, Double, Double,Double, Double>> o1,
                       Tuple2<OffloadScheduling, Tuple5<Integer, Double, Double,Double, Double>> o2) {
        /*
         *  Deployments are characterized by a Tuple2, whose values are a OffloadScheduling and a Tuple5
         *  containing all its execution parameters (frequency, runtime, cost, battery lifetime, execution time);
         *  since we are interested in the frequency, we select the Tuple5 ( o1._2() )
         *  and we pick its first value ( _1() )
         */
        return o1._2()._1() - o2._2()._1();
    }

}
