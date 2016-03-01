/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.utils;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author tom
 */
public class Anonymizer {

	public static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA1";

	// The following constants may be changed without breaking existing hashes.
	public static final int SALT_BYTE_SIZE = 4;
	public static final int HASH_BYTE_SIZE = 8;
	public static final int PBKDF2_ITERATIONS = 3;

	public static final int ITERATION_INDEX = 0;
	public static final int SALT_INDEX = 1;
	public static final int PBKDF2_INDEX = 2;

	private final SecureRandom random;
	private final byte[] salt;
	private final long timeSalt;

	public Anonymizer() {
		random = new SecureRandom();
		salt = new byte[SALT_BYTE_SIZE];

		timeSalt = random.nextInt(1000000000) + 1000000000;
		random.nextBytes(salt);
	}

	public String anonymize(String str)
		throws NoSuchAlgorithmException, InvalidKeySpecException {

		// Hash the password
		PBEKeySpec spec = new PBEKeySpec(str.toCharArray(), salt, PBKDF2_ITERATIONS, HASH_BYTE_SIZE * 8);
		SecretKeyFactory skf = SecretKeyFactory.getInstance(PBKDF2_ALGORITHM);
		byte[] hash = skf.generateSecret(spec).getEncoded();

		return Hex.encodeHexString(hash);
	}

	/**
	 * hash the input string but keep extension
	 *
	 * "http://www.google.com/support/enterprise/static/gsa/docs/admin/70/gsa_doc_set/integrating_apps/images/google_logo.png";
	 * would become something like fe29e95f11e85425.png
	 *
	 * @param str
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeySpecException
	 */
	public String anonymizeKeepExtension(String str)
		throws NoSuchAlgorithmException, InvalidKeySpecException {

		String extension = "";
		String pathToHash = "";

		if (str != null && str.contains(".")) {
			int dotId = str.lastIndexOf(".");
			extension = str.substring(dotId);
			pathToHash = str.substring(0, dotId - 1);
		}

		return anonymize(pathToHash) + extension;
	}

	public Date anonymize(Date d) {
		long newTimestamp = d.getTime() - timeSalt;
		
		if (newTimestamp > 0) {
			return new Date(d.getTime() - timeSalt);
		} else {
			return d;
		}
	}

	public static void main(String[] args) {
		try {

			Anonymizer anon = new Anonymizer();

			String ip1 = "123.90.3.23";
			String ip2 = "123.90.3.24";
			System.out.println(anon.anonymize(ip1));

			System.out.println(anon.anonymize(ip1));
			System.out.println(anon.anonymize(ip2));

			String url = "http://www.google.com/support/enterprise/static/gsa/docs/admin/70/gsa_doc_set/integrating_apps/images/google_logo.png";

			System.out.println(anon.anonymizeKeepExtension(url));

		} catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
			Logger.getLogger(Anonymizer.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
