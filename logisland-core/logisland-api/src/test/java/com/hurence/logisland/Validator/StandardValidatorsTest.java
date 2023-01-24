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
package com.hurence.logisland.Validator;

import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;

/**
 * @author greg
 */
public class StandardValidatorsTest {


    @Test
    public void booleanValidator() throws IOException {
        Validator boolV = StandardValidators.BOOLEAN_VALIDATOR;
        ValidationResult result = boolV.validate(null, "true");
        Assert.assertTrue("'true' should be validated as a boolean", result.isValid());
        result = boolV.validate(null, "false");
        Assert.assertTrue("'false' should be validated as a boolean",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("'aaa' should not be validated as a boolean",result.isValid());
        result = boolV.validate(null, "0");
        Assert.assertFalse("a number should not be validated as a boolean",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a boolean",result.isValid());
    }
    @Test
    public void doubleValidator() throws IOException {
        Validator boolV = StandardValidators.DOUBLE_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a double", result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertTrue("'1.356' should be validated as a double",result.isValid());
        //TODO should this case be implemented ? internationnal norms
//        result = boolV.validate(null, "1,356");
//        Assert.assertTrue("'1,356' should be validated as a double",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a double",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a double",result.isValid());
    }
    @Test
    public void floatValidator() throws IOException {
        Validator boolV = StandardValidators.FLOAT_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a float", result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertTrue("'1.356' should be validated as a float",result.isValid());
        //TODO should this case be implemented ? internationnal norms
//        result = boolV.validate(null, "1,356");
//        Assert.assertTrue("'1,356' should be validated as a double",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a float",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a float",result.isValid());
    }
    @Test
    public void positiveIntegerValidator() throws IOException {
        Validator boolV = StandardValidators.POSITIVE_INTEGER_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a positive integer", result.isValid());
        result = boolV.validate(null, "0");
        Assert.assertFalse("'0' should not be validated as a positive integer",result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertFalse("'1.356' should not be validated as a positive integer",result.isValid());
        result = boolV.validate(null, "1,356");
        Assert.assertFalse("'1,356' should not be validated as a positive integer",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a positive integer",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertFalse("a negative number should not be validated as a positive integer",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a positive integer",result.isValid());
    }
    @Test
    public void positiveLongValidator() throws IOException {
        Validator boolV = StandardValidators.POSITIVE_LONG_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a positive long", result.isValid());
        result = boolV.validate(null, "0");
        Assert.assertFalse("'0' should not be validated as a positive long",result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertFalse("'1.356' should not be validated as a positive long",result.isValid());
        result = boolV.validate(null, "1,356");
        Assert.assertFalse("'1,356' should not be validated as a positive long",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a positive long",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertFalse("a negative number should not be validated as a positive long",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a positive long",result.isValid());
    }
    @Test
    public void portValidator() throws IOException {
        Validator boolV = StandardValidators.PORT_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a port", result.isValid());
        result = boolV.validate(null, "0");
        Assert.assertFalse("'0' should not be validated as a port",result.isValid());
        result = boolV.validate(null, "65535");
        Assert.assertTrue("'65535' should be validated as a port",result.isValid());
        result = boolV.validate(null, "65536");
        Assert.assertFalse("'65536' should not be validated as a port",result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertFalse("'1.356' should not be validated as a port",result.isValid());
        result = boolV.validate(null, "1,356");
        Assert.assertFalse("'1,356' should not be validated as a port",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a port",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertFalse("a negative number should not be validated as a port",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a port",result.isValid());
    }
    @Test
    public void nonEmptyValidator() throws IOException {
        Validator boolV = StandardValidators.NON_EMPTY_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a non empty string", result.isValid());
        result = boolV.validate(null, "");
        Assert.assertFalse("'' should not be validated as a non empty string",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertTrue("'aaa' should be validated as a non empty string",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a non empty string",result.isValid());
    }
    @Test
    public void integerValidator() throws IOException {
        Validator boolV = StandardValidators.INTEGER_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as an integer", result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertFalse("'1.356' should not be validated as an integer",result.isValid());
        result = boolV.validate(null, "1,356");
        Assert.assertFalse("'1,356' should not be validated as an integer",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as an integer",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertTrue("a negative number should be validated as an integer",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as an integer",result.isValid());
    }
    @Test
    public void longValidator() throws IOException {
        Validator boolV = StandardValidators.LONG_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a positive long", result.isValid());
        result = boolV.validate(null, "1.356");
        Assert.assertFalse("'1.356' should not be validated as a long",result.isValid());
        result = boolV.validate(null, "1,356");
        Assert.assertFalse("'1,356' should not be validated as a long",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a long",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertTrue("a negative number should be validated as a long",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a long",result.isValid());
    }
    @Test
    public void nonNegativeIntegerValidator() throws IOException {
        Validator boolV = StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a non negatif integer", result.isValid());
        result = boolV.validate(null, "0");
        Assert.assertTrue("'0' should not be validated as a non negatif integer",result.isValid());
        result = boolV.validate(null, "-1");
        Assert.assertFalse("'-1' should not be validated as a non negatif integer",result.isValid());
        result = boolV.validate(null, "aaa");
        Assert.assertFalse("a string should not be validated as a non negatif integer",result.isValid());
        result = boolV.validate(null, "-8765");
        Assert.assertFalse("'-8765' should be validated as a non negatif integer",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a non negatif integer",result.isValid());
    }
    @Test
    public void commaSeparatedListValidator() throws IOException {
        Validator boolV = StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR;
        ValidationResult result = boolV.validate(null, "1");
        Assert.assertTrue("'1' should be validated as a positive comma separated list", result.isValid());
        result = boolV.validate(null, "1.356,egtr,greh");
        Assert.assertTrue("'1.356,egtr,greh' should be validated as a comma separated list",result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a comma separated list",result.isValid());
        result = boolV.validate(null, "");
        Assert.assertTrue("empty string should be validated as a comma separated list",result.isValid());
    }

    @Test
    public void characterSetValidator() throws IOException {
        Validator boolV = StandardValidators.CHARACTER_SET_VALIDATOR;
        //TODO
    }

    @Test
    public void urlValidator() throws IOException {
        Validator boolV = StandardValidators.URL_VALIDATOR;
        //TODO
    }

    @Test
    public void uriValidator() throws IOException {
        Validator boolV = StandardValidators.URI_VALIDATOR;
        //TODO
    }

    @Test
    public void fileExistValidator() throws IOException {
        Validator boolV = StandardValidators.FILE_EXISTS_VALIDATOR;
        //TODO
    }

    @Test
    public void hashAlgorithmValidator() throws IOException {
        Validator boolV = StandardValidators.HASH_ALGORITHM_VALIDATOR;
        ValidationResult result = boolV.validate(null, "SHA-256");
        Assert.assertTrue("'SHA-256' should be validated as an hash algorithm", result.isValid());
        result = boolV.validate(null, "1");
        Assert.assertFalse("'1' should not be validated as an hash algorithm", result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as an hash algorithm", result.isValid());
    }

    @Test
    public void languageTagValidator() throws IOException {
        Validator boolV = StandardValidators.LANGUAGE_TAG_VALIDATOR;
        ValidationResult result = boolV.validate(null, "SHA-256");
        Assert.assertFalse("'SHA-256' should not be validated as a language tag", result.isValid());
        result = boolV.validate(null, "1");
        Assert.assertFalse("'1' should not be validated as a language tag", result.isValid());
        result = boolV.validate(null, null);
        Assert.assertFalse("null should not be validated as a language tag", result.isValid());
        result = boolV.validate(null, Locale.ENGLISH.toLanguageTag());
        Assert.assertTrue("english tag should be validated as a language tag", result.isValid());
    }

    @Test
    public void filterRegexpValidator() throws IOException {
        Validator boolV = StandardValidators.FILTER_REGEXP_VALIDATOR;
        ValidationResult result = boolV.validate(null, "nofield");
        Assert.assertFalse("'nofield' does not has the form 'key:<regexp>'", result.isValid());
        result = boolV.validate(null, "nofield:");
        Assert.assertFalse("'nofield:' does not has the form 'key:<regexp>'", result.isValid());
        result = boolV.validate(null, "nofield:\\d{f");
        Assert.assertFalse("'nofield:\\d{f' invalid regular expression", result.isValid());
        result = boolV.validate(null, "nofield:\\d+");
        Assert.assertTrue("'nofield:'\\d+' is compliant with the form 'key:<regexp>'", result.isValid());
    }

}