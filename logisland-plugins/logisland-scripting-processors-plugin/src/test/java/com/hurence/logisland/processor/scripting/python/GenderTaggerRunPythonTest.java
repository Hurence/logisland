/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.scripting.python;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenderTaggerRunPythonTest {
    
    private static Logger logger = LoggerFactory.getLogger(GenderTaggerRunPythonTest.class);
    
    private static final String PYTHON_PROCESSOR = "./src/main/python/processors/gendertagger/GenderTaggerProcessor.py";
    
    // Firstname -> Gender
    private static Map<String, String> recordAuthorsToGender = new HashMap<String, String>();
    
    private static final String MALE = "male";
    private static final String FEMALE = "female";
    
    private static final String AUTHOR_FIRSTNAME_TAG = "author_firstname";
    private static final String AUTHOR_GENDER_TAG = "author_gender";
    
    static
    {
        recordAuthorsToGender.put("Ben", MALE);
        recordAuthorsToGender.put("John", MALE);
        recordAuthorsToGender.put("Bob", MALE);
        recordAuthorsToGender.put("William", MALE);
        recordAuthorsToGender.put("Andrew", MALE);
        recordAuthorsToGender.put("Fançois", MALE);
        recordAuthorsToGender.put("Scott", MALE);
        recordAuthorsToGender.put("Mathieu", MALE);
        recordAuthorsToGender.put("Cyril", MALE);
        recordAuthorsToGender.put("Vernon", MALE);
        recordAuthorsToGender.put("Antonia", FEMALE);
        recordAuthorsToGender.put("Paulita", FEMALE);
        recordAuthorsToGender.put("Jacquelyn", FEMALE);
        recordAuthorsToGender.put("Earlie", FEMALE);
        recordAuthorsToGender.put("Viki", FEMALE);
        recordAuthorsToGender.put("Laurence", FEMALE);
        recordAuthorsToGender.put("Anna", FEMALE);
        recordAuthorsToGender.put("Isabelle", FEMALE);
        recordAuthorsToGender.put("Julie", FEMALE);
        recordAuthorsToGender.put("Anna", FEMALE);
    }

    @Test
    public void testGenderTagger() {
        
        final TestRunner testRunner = TestRunners.newTestRunner(new RunPython());
        testRunner.setProperty(RunPython.SCRIPT_PATH, PYTHON_PROCESSOR);
        testRunner.assertValid();
        int nRecords = recordAuthorsToGender.size();
        /**
         * Enqueue some records with their author firstname as tag
         */
        for (Map.Entry<String, String> entry : recordAuthorsToGender.entrySet())
        {
            Record record = new StandardRecord("simple_record");
            record.setStringField(AUTHOR_FIRSTNAME_TAG, entry.getKey());
            testRunner.enqueue(record);
        }
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(nRecords);
        
        int nCorrectGuess = 0;
        
        for (int i = 0; i < nRecords ; i++)
        {
            MockRecord out = testRunner.getOutputRecords().get(i);
            out.assertFieldExists(AUTHOR_FIRSTNAME_TAG);
            String authorFirstname = out.getField(AUTHOR_FIRSTNAME_TAG).getRawValue().toString();
    
            out.assertFieldExists(AUTHOR_GENDER_TAG);
    
            // Check that the record has been tagged with the expected gender of the author
            String expectedGender = recordAuthorsToGender.get(authorFirstname);
            String guessedGender = out.getField(AUTHOR_GENDER_TAG).getRawValue().toString();
            
            if (guessedGender.equals(expectedGender))
            {
                nCorrectGuess++; 
            } else
            {
                System.out.println("Prediction error for " + authorFirstname);
            }
            
            out.assertRecordSizeEquals(2);
        }
        
        // Check that the accuracy of prediction is higher than the expected ratio
        float ratio =  (float)nCorrectGuess/(float)nRecords;
        float expectedMinRatio = (float)0.70;
        System.out.println("Effective prediction accuracy " + ratio);
        assertTrue("The gender tagger accuracy is lower that the expected minimum value. Expected at least "
                + expectedMinRatio + " but got "  + ratio, ratio >=  expectedMinRatio);
    }

}
