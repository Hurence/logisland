/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.utils.string;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import static org.junit.Assert.*;

public class HttpUtilsTest {

	@Test
	public void testGetQueryKeys() {
		try {
			String url = "http://validator.w3.org/feed/check.cgi?url=http://feeds.feedburner.com/domainfeed";
			URI uri = new URI(url);

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(1, keys.size());
			assertEquals(1, values.size());

			assertTrue(keys.get(0).equals("url"));
			assertTrue(values.get(0).equals("http://feeds.feedburner.com/domainfeed"));
		} catch (URISyntaxException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Test
	public void testNullQueryKeys() {
		try {
			String url = "http://validator.w3.org/feed/check.cgi";
			URI uri = new URI(url);

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(0, keys.size());
			assertEquals(0, values.size());
		} catch (URISyntaxException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Test
	public void testSpecialQueryKeys() {
		try {
			String url = "http://g.microsoft.com/_0sfdata/1?CG={2CEDBFBC-DBA8-43AA-B1FD-CC8E6316E3E2}&DV=8.0.6001.9&OS=5.1.2600.3.0&BL=fr&AA=0:05:34:19&AB=10586&AC=86&AD=7&AE=38&AF=41&AG=160&AH=0&AI=24&AJ=800&AK=0&AL=8&NR=154&BD=0&NE=0&IU=0&SD=0&NO=0&BS=0&OE=0&UA=0&TP=2&TC=78&TE=1&NP=817";

			URI uri = HttpUtils.getURIFromEncodedString(url);
			
			

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(29, keys.size());
			assertEquals(29, values.size());
		} catch (URISyntaxException | UnsupportedEncodingException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	
	@Test
	public void testSpecialQueryKeys2() {
		try {
			String url ="http://sqlserverplanet.com/troubleshooting/[template-url]/images/ico5.png";

			URI uri = HttpUtils.getURIFromEncodedString(url);
			
			

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(0, keys.size());
			assertEquals(0, values.size());
		} catch (URISyntaxException | UnsupportedEncodingException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	
	
	@Test
	public void testSpecialQueryKeys3() {
		try {
			String url = "http://fr-mg42.mail.yahoo.com/neo/ult/ultLog?=&storm=1&sid=978524091&rand=617325&ck=%7B%22_S%22%3A%22978524091%22%2C%22_s%22%3A%22978524091%22%7D&pg=%7B%22t1%22%3A%22spmTb%22%2C%22pp%22%3Afalse%2C%22mpp%22%3A%2225%22%2C%22intl%22%3A%22fr%22%2C%22lang%22%3A%22fr-FR%22%2C%22mt%22%3A%22free%22%2C%22pt%22%3A%22%22%2C%22bldVer%22%3A%223.54.0.3611%22%2C%22test%22%3A%22CMUSGRMBPC07%22%7D&config=%7B%22ln%22%3A%7B%22action%22%3A%22folderview_bulk%22%2C%22pp%22%3Afalse%2C%22append%22%3A%7B%7D%2C%22thrd%22%3A1%7D%7D";

			URI uri = HttpUtils.getURIFromEncodedString(url);
			
			

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(6, keys.size());
			assertEquals(6, values.size());
		} catch (URISyntaxException | UnsupportedEncodingException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	
	
	@Test
	public void testSpecialQueryKeys4() {
		try {
			String url ="http://ad.caradisiac-publicite.com/addyn|3.0|1286.1|3620357|0|-1|size=969x32ADTECHloc=100alias=caradisiac_modele_home_Stationtarget=_blankkvautre=C3_PICASSOkvmarq=CITROENkvmodel=C3_PICASSOkvcat=44grp=742misc=1388660962373";
	

			URI uri = HttpUtils.getURIFromEncodedString(url);
			
			

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(0, keys.size());
			assertEquals(0, values.size());
		} catch (URISyntaxException | UnsupportedEncodingException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	@Test
	public void testSpecialQueryKeys5() {
		try {
			String url ="http://wzeu.ask.com/r?t=p&d=eu&s=fr&c=a&app=a16&dqi=&askid=&l=sem&o=10123&oo=0&sv=0a6d00dc&ip=5076a27e&id=85832001A977B8F0DE762FD74EB4176B&q=sp%C3%A9cialit%C3%A9s+vietnamienne+bijouxs&p=1&qs=999&ac=24&g=445c5uJeS9U2%I&ocq=0&ocp=0&ocu=0&ocf=0&qa1=0&cu.wz=0&en=te&io=0&b=a004&tp=d&ec=1&ex=tsrc%3Dtled&pt=Lire%20la%20suite%20%C2%BB&u=http%3A%2F%2Fwww.cercoop.org%2FIMG%2Fpdf%2FGuide_accueil_Vietnam_Sept_2011.pdf";
	

			URI uri = HttpUtils.getURIFromEncodedString(url);
			
			

			List<String> keys = new ArrayList<>();
			List<String> values = new ArrayList<>();
			
			HttpUtils.populateKeyValueListFromUrlQuery(uri.getQuery(), keys, values);

			assertEquals(32, keys.size());
			assertEquals(31, values.size());
		} catch (URISyntaxException | UnsupportedEncodingException ex) {
			Logger.getLogger(HttpUtilsTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	
}
	
	
	
