package ${package};

import com.hurence.logisland.event.Event;
import com.hurence.logisland.log.LogParser;
import com.hurence.logisland.log.LogParserException;

import java.text.SimpleDateFormat;

/**
 * NetworkFlow(
 * timestamp: Long,
 * method: String,
 * ipSource: String,
 * ipTarget: String,
 * urlScheme: String,
 * urlHost: String,
 * urlPort: String,
 * urlPath: String,
 * requestSize: Int,
 * responseSize: Int,
 * isOutsideOfficeHours: Boolean,
 * isHostBlacklisted: Boolean,
 * tags: String)
 */
public class ProxyLogParser implements LogParser {

    /**
     * take a line of csv and convert it to a NetworkFlow
     *
     * @param s
     * @return
     */
    public Event[] parse(String s) throws LogParserException {


        Event event = new Event();

        try {
            String[] records = s.split("\t");

            try {
                SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                event.put("timestamp", "long", sdf.parse(records[0]).getTime());
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }

            event.put("method", "string", records[1]);
            event.put("ipSource", "string", records[2]);
            event.put("ipTarget", "string", records[3]);
            event.put("urlScheme", "string", records[4]);
            event.put("urlHost", "string", records[5]);
            event.put("urlPort", "string", records[6]);
            event.put("urlPath", "string", records[7]);

            try {
                event.put("requestSize", "int", Integer.parseInt(records[8]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("responseSize", "int", Integer.parseInt(records[9]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("isOutsideOfficeHours", "bool", Boolean.parseBoolean(records[10]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }
            try {
                event.put("isHostBlacklisted", "bool", Boolean.parseBoolean(records[11]));
            } catch (Exception e) {
                event.put("parsing_error", e.getMessage());
            }


            if (records.length == 13) {
                String tags = records[12].replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "");
                event.put("tags", "string", tags);
            }


        }catch (Exception e) {
            event.put("parsing_error", e.getMessage());
        }

        Event[] result = new Event[1];
        result[0] = event;

        return result;
    }

}
