package stormTP.operator;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CompteBonusBolt implements IRichBolt {

    private OutputCollector collector;

    // État conservé (stateful)
    private int score = 0;
    private int topDebut = -1;

    @Override
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            int id = t.getIntegerByField("id");
            int top = t.getIntegerByField("top");
            String rang = t.getStringByField("rang");
            int total = t.getIntegerByField("total");
            int rangInt = Integer.parseInt(rang.replace("ex", ""));
            // Calcul uniquement tous les 15 tops
            if (top % 15 == 0) {

                if (topDebut == -1) {
                    topDebut = top;
                }

                int points = total - rangInt;
                score += points;

                String tops = topDebut + "-" + top;

                collector.emit(new Values(id, tops, score));
            }

            collector.ack(t);

        } catch (Exception e) {
            System.err.println("Erreur dans ComputeBonusBolt : " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "score"));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
