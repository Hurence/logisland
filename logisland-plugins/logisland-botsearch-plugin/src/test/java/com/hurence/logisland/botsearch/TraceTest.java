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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.botsearch;

import com.hurence.logisland.util.time.DateUtil;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class TraceTest {

    private static Trace getSampleTrace() {
        Trace t = new Trace();
        
        
        String[] flows
                = {"Thu Jan 02 08:43:39 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-2013090.13.026/Images/RightJauge.gif	724	409	false	false",
                    "Thu Jan 02 08:43:40 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-2013090.13.026/Images/fondJauge.gif	723	402	false	false",
                    "Thu Jan 02 08:43:42 CET 2014	GET	10.118.32.164	193.252.23.209	http	static1.lecloud.wanadoo.fr	80	/home/fr_FR/20131202100641/img/sprite-icons.pn	495	92518	false	false",
                    "Thu Jan 02 08:43:43 CET 2014	GET	10.118.32.164	173.194.66.94	https	www.google.fr	443	/complete/search	736	812	false	false",
                    "Thu Jan 02 08:43:45 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-2013090.13.026/Images/digiposte/archiver-btn.png	736	2179	false	false",
                    "Thu Jan 02 08:43:49 CET 2014	GET	10.118.32.164	193.251.214.117	http	webmail.laposte.net	80	/webmail/fr_FR/Images/Images-2013090.13.026/Images/picto_trash.gif	725	544	false	false"};

        for (String flowString : flows) {
            String[] split = flowString.split("\t");
            HttpFlow flow = new HttpFlow();
            try {
                flow.setDate(DateUtil.fromLegacyStringToDate(split[0]));
                flow.setipSource(split[2]);
                flow.setRequestSize(Long.parseLong(split[8]));
                flow.setResponseSize(Long.parseLong(split[9]));
            } catch (ParseException ex) {
                Logger.getLogger(TraceTest.class.getName()).log(Level.SEVERE, null, ex);
            }
            t.add(flow);

        }
        return t;
    }

 
    @Test(expected = IllegalArgumentException.class)
    public void testComputeVoid() {
        System.out.println("computeVoid");

        Trace trace = new Trace();

        trace.compute();
    }

 
    @Test
    public void testCompute() {
        System.out.println("compute");
        Trace instance = getSampleTrace();
                
        instance.compute();
        
        // durations 1 2 1 2 4
        assertTrue( instance.getFlows().size() == 6);
    }

    /**
     * Test of sampleFlows method, of class Trace.
     */
    //@Test
    public void testSampleFlows() {
        System.out.println("sampleFlows");
        Trace instance = new Trace();
        double[] expResult = null;
        double[] result = instance.sampleFlows();
        assertArrayEquals(expResult, result, 0.01);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of computePowerSpectralDensity method, of class Trace.
     */
    //@Test
    public void testComputePowerSpectralDensity() {
        System.out.println("computePowerSpectralDensity");
        double[] samples = null;
        Trace instance = new Trace();
        double[] expResult = null;
        double[] result = instance.computePowerSpectralDensity(samples);
        assertArrayEquals(expResult, result, 0.01);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of smoothArray method, of class Trace.
     */
    //@Test
    public void testSmoothArray() {
        System.out.println("smoothArray");
        double[] values = null;
        double smoothing = 0.0;
        Trace instance = new Trace();
        instance.smoothArray(values, smoothing);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}
