package com.hurence.logisland.webanalytics.test.util;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class EventsGenerator {

    final String sessionId;

    public EventsGenerator(String sessionId) {
        this.sessionId = sessionId;
    }

    public Record generateEvent(long timestamp, String url) {
        Record record = new StandardRecord("generated");
        record.setStringField(TestMappings.eventsInternalFields.getSessionIdField(), sessionId);
        record.setLongField(TestMappings.eventsInternalFields.getTimestampField(), timestamp);
        record.setStringField(TestMappings.eventsInternalFields.getVisitedPageField(), url);
        record.setStringField(TestMappings.eventsInternalFields.getUserIdField(), "greg");
        record.setStringField("Company", "orexad");
        record.setStringField("codeProduct_rev", "fake_product");
        record.setStringField("categoryNiv4", "fakeniv4");

        return record;

//    { "version": 1,
//            "name": "io.divolte.examples.record",
//            "type": "record",
//            "fields": [
//        { "name": "h2kTimestamp",            "type": "long" },
//        { "name": "remoteHost",              "type": "string"},
//        { "name": "record_type",             "type": ["null", "string"], "default": null },
//        { "name": "record_id",               "type": ["null", "string"], "default": null },
//        { "name": "location",                "type": ["null", "string"], "default": null },
//        { "name": "hitType",                 "type": ["null", "string"], "default": null },
//        { "name": "eventCategory",           "type": ["null", "string"], "default": null },
//        { "name": "eventAction",             "type": ["null", "string"], "default": null },
//        { "name": "eventLabel",              "type": ["null", "string"], "default": null },
//        { "name": "localPath",               "type": ["null", "string"], "default": null },
//        { "name": "q",                       "type": ["null", "string"], "default": null },
//        { "name": "n",                       "type": ["null", "int"],    "default": null },
//        { "name": "referer",                 "type": ["null", "string"], "default": null},
//        { "name": "viewportPixelWidth",      "type": ["null", "int"],    "default": null},
//        { "name": "viewportPixelHeight",     "type": ["null", "int"],    "default": null},
//        { "name": "screenPixelWidth",        "type": ["null", "int"],    "default": null},
//        { "name": "screenPixelHeight",       "type": ["null", "int"],    "default": null},
//        { "name": "partyId",                 "type": ["null", "string"], "default": null},
//        { "name": "sessionId",               "type": ["null", "string"], "default": null},
//        { "name": "pageViewId",              "type": ["null", "string"], "default": null},
//        { "name": "is_newSession",           "type": ["null", "boolean"], "default": null},
//        { "name": "userAgentString",         "type": ["null", "string"], "default": null},
//        { "name": "pageType",                "type": ["null", "string"], "default": null},
//        { "name": "Userid",                  "type": ["null", "string"], "default": null},
//        { "name": "B2BUnit",                 "type": ["null", "string"], "default": null},
//        { "name": "pointOfService",          "type": ["null", "string"], "default": null},
//        { "name": "companyID",               "type": ["null", "string"], "default": null},
//        { "name": "GroupCode",               "type": ["null", "string"], "default": null},
//        { "name": "userRoles",               "type": ["null", "string"], "default": null},
//        { "name": "is_PunchOut",             "type": ["null", "string"], "default": null},
//        { "name": "codeProduct",             "type": ["null", "string"], "default": null},
//        { "name": "categoryProductId",       "type": ["null", "string"], "default": null},
//        { "name": "categoryName",            "type": ["null", "string"], "default": null},
//        { "name": "categoryCode",            "type": ["null", "string"], "default": null},
//        { "name": "categoryNiv5",            "type": ["null", "string"], "default": null},
//        { "name": "categoryNiv4",            "type": ["null", "string"], "default": null},
//        { "name": "categoryNiv3",            "type": ["null", "string"], "default": null},
//        { "name": "categoryNiv2",            "type": ["null", "string"], "default": null},
//        { "name": "categoryNiv1",            "type": ["null", "string"], "default": null},
//        { "name": "countryCode",             "type": ["null", "string"], "default": null},
//        { "name": "Company",                 "type": ["null", "string"], "default": null},
//        { "name": "is_Link",                 "type": ["null", "boolean"], "default": null},
//        { "name": "clickText",               "type": ["null", "string"], "default": null},
//        { "name": "clickURL",                "type": ["null", "string"], "default": null},
//        { "name": "newCustomerWebOnly",      "type": ["null", "int"], "default": null},
//        { "name": "newCustomerWebOrAgency",  "type": ["null", "int"], "default": null},
//        { "name": "productPrice",            "type": ["null", "float"], "default": null},
//        { "name": "searchedProductIndex",    "type": ["null", "int"], "default": null},
//        { "name": "searchedProductPage",     "type": ["null", "int"], "default": null},
//        { "name": "stockInfo",               "type": ["null", "string"], "default": null},
//        { "name": "transactionId",           "type": ["null", "string"], "default": null},
//        { "name": "transactionTotal",        "type": ["null", "float"], "default": null},
//        { "name": "transactionCurrency",     "type": ["null", "string"], "default": null},
//        { "name": "productQuantity",         "type": ["null", "int"], "default": null},
//        { "name": "Alt",                     "type": ["null", "string"], "default": null},
//        { "name": "erpLocaleCode",           "type": ["null", "string"], "default": null},
//        { "name": "salesOrg",                "type": ["null", "string"], "default": null},
//        { "name": "country",                 "type": ["null", "string"], "default": null},
//        { "name": "currency",                "type": ["null", "string"], "default": null},
//        { "name": "currentCart",             "type": ["null", {"type": "array", "items":{
//            "name": "Product", "type": "record", "fields":[
//            {"name": "price", "type": "double"},
//            {"name": "quantity", "type": "int"},
//            {"name": "sku", "type": "string"}
//                  ]}
//                }],
//            "default": null
//        }
//            ]
//    }
    }

    public List<Record> generateEvents(List<Long> timestamps,
                                       String url) {
        return timestamps.stream()
                .map(ts -> generateEvent(ts, url))
                .collect(Collectors.toList());
    }

    public List<Record> generateEvents(Long from,
                                       Long to,
                                       Long padding) {
        int numberOfRecord = (int)((to - from) / padding);
        return LongStream.iterate(from, ts -> ts + padding)
                .limit(numberOfRecord)
                .mapToObj(ts -> generateEvent(ts, "url"))
                .collect(Collectors.toList());
    }

    public List<Record> generateEventsRandomlyOrdered(Long from,
                                       Long to,
                                       Long padding) {
        List<Record> sortedList = generateEvents(from, to, padding);
        Collections.shuffle(sortedList);;
        return sortedList;
    }
}
