package stormTP.operator;


import java.util.Map;
//import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;

public class Exit4Bolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107342L;

    private OutputCollector collector;
    private int port = -1;
    private StreamEmiter semit = null;

    public Exit4Bolt(int port) {
        this.port = port;
        this.semit = new StreamEmiter(this.port);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public void execute(Tuple t) {
        int id = t.getIntegerByField("id");
        String tops = t.getStringByField("tops");
        int score = t.getIntegerByField("score");
        // Construction du JSON
        JsonObjectBuilder jsonC = Json.createObjectBuilder();
        jsonC.add("id", id);
        jsonC.add("top", tops);
        jsonC.add("score", score);
        JsonObject objetFinal = jsonC.build();
        String jsonFinal = objetFinal.toString();
        //envoyer sur le port 
        this.semit.send(jsonFinal);
        collector.emit(new Values(jsonFinal));
        collector.ack(t);

    }
}
