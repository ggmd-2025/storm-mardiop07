package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.Exit4Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.CompteBonusBolt;

/**
 * Topologie T3 : 
 * InputStreamSpout -> GiveRankBolt -> Exit3Bolt
 */
public class TopologyT4 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        
        // Création du spout
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);

        // Création de la topologie
        TopologyBuilder builder = new TopologyBuilder();

        // Spout source
        builder.setSpout("masterStream", spout);
 
        //Bolt MyTortoise
        builder.setBolt("tortoise", new MyTortoiseBolt(3, "diopsail"), nbExecutors)
       .shuffleGrouping("masterStream");
        

        //Bolt GiveRank
        builder.setBolt("rank", new GiveRankBolt(), nbExecutors)
       .shuffleGrouping("tortoise");

       

       //Bolt CompteBonus
       builder.setBolt("bonus", new CompteBonusBolt(), nbExecutors)
        .shuffleGrouping("rank");

        //Exit4Bolt 
        builder.setBolt("exit4", new Exit4Bolt(portOUTPUT), nbExecutors)
       .shuffleGrouping("bonus");
       
        // Configuration
        Config config = new Config();

        // Soumission de la topologie
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }
}