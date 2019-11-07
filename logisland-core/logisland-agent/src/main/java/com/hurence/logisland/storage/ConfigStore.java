package com.hurence.logisland.storage;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.dizitart.no2.objects.ObjectRepository;

import java.util.ArrayList;
import java.util.Date;

import static org.dizitart.no2.Document.createDocument;
import static org.dizitart.no2.objects.filters.ObjectFilters.eq;

public class ConfigStore {


    public static void main(String[] args) {

        // java initialization
        Nitrite db = Nitrite.builder()
                .compressed()
                .filePath("./test.db")
                .openOrCreate("user", "password");

        // Create a Nitrite Collection
        NitriteCollection collection = db.getCollection("test");

        // Create an Object Repository
        ObjectRepository<Employee> repository = db.getRepository(Employee.class);


        // create a document to populate data
        Document doc = createDocument("firstName", "John")
                .put("lastName", "Doe")
                .put("birthDay", new Date())
                .put("data", new byte[]{1, 2, 3})
                .put("fruits", new ArrayList<String>() {{
                    add("apple");
                    add("orange");
                    add("banana");
                }})
                .put("note", "a quick brown fox jump over the lazy dog");

        // insert the document
        collection.insert(doc);

        // update the document
        collection.update(eq("firstName", "John"), createDocument("lastName", "Wick"));

        // remove the document
        collection.remove(doc);


    }

    private static class Employee {
    }
}
