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
package com.hurence.logisland.connect.opc.ua;

import com.hurence.opc.auth.X509Credentials;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class OpcUaSourceTaskTest {

    @Test
    public void testCredentialLoad() throws Exception {


        String key = "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIJKAIBAAKCAgEAuFooF2RLI/v3z01grK6UDpi3efJO2EI/w93gD/61r486LB3J\n" +
                "Wk2gffNgJZEpaQJTE/gcIN4faPIcAn5/ZvzXZ+LgzpAOByq/Xo6zjp/QqZhsBjyU\n" +
                "2+DrSU2dga8CtA9KLkkRtM2z4w1eMeTDq8D0IvOPPgxPSmyY1Iju7LtoYRLhSgg1\n" +
                "zNgOskkOwJPgpl89dwVYDSQqTeUhVyWNH0hmxT8VxgG7U+VRBPoIr7NLgNBF5VMB\n" +
                "VJPEqsAmeG3fMmjxYgjOeXRQDLWHVboKv1OnTbYvfDqH8V0S8MrYOqp4afu4ogcv\n" +
                "Oj8kfQ0eeqiSMy1s2vssupzhiXHwAViLAPyMBaxGIai157rBXVf+c8FJG+Vklmb3\n" +
                "WYs5uMwRsXNc86+RfoWSxBzKvPqx0aA64tsFZMPD/59Dk2Rj2xVs1gD64X38ucMJ\n" +
                "16CG/1BDlkwmfmWPpKeB/ALFmQuIXZFHMUnzkI8AJiWP6hjo3yGnHFwBe36OHyHG\n" +
                "YrCWX/km/rrmLV4AG04neSZ1zxeKMTleLTUg5cdW8NQzOywe0m1NIG9RZ99WdTsA\n" +
                "qkyqJ1tnwbup6j2UVvszz6O7uRNso260Xn9GKPZRKUJHW4aPTPPQOYgRvBhqSlhw\n" +
                "X1AuvPREdzvIfnrqj4jmhWS7MMtFK3+oFC1wI8kBjClAaQ0yNDfeXBP/nC8CAwEA\n" +
                "AQKCAgEAtF0IsnIejfM8LWa/+dLH6kwB3l5yQ2T1q/UM/bkvGrdfq7/sutwN9IxD\n" +
                "eh2+zQ1IKNZq9sE7K9sMCmimzyT6vpobZh1MjDiHiMTG6fh0FymYLrXg0gsJR+uW\n" +
                "+UU3uODoq8Yze5hxsefnS5tM0WJzuSpf783tWZxMHkxmrdhhM/Bb2KmVsXeFUWrm\n" +
                "8wT7Gus9YJAq6JiEhzdw2ilUG9IjMkIZVGNnWpqWHO9fxj791OZwLAB84bm9BW3/\n" +
                "dX3RjCleWJLTJ8Ljeruzz+y4DR6UJhTj+n/tdvifylQ7H5KfQtnTdzreOveCBJLs\n" +
                "SgdZGpcL1GdACMfqZSXDMh3lya5Mcqo7UgKM4o0dHcW5HMALS6SMcGmMyzapVP6k\n" +
                "a05ebuo0fNuHcgl+Rf5/QukEje+zfK3yfJKAhUeO/A5wMDIl4uruIOImR1N3imdG\n" +
                "Z7QbzSFYjnmUXRO0ZCP3rvoNsWxRRgRTAw/Tk0QRTTtpGiqq0Kwcj7OwRCX33Vlc\n" +
                "t4jg++fNw3iPC2NL9VFP0maeypNpZaxK/T+GuqCyagvYgyrQUGEsWv0hg8Myvji2\n" +
                "6tsa9aZDI+GwR4m/uZwtELR+nTdqDpxQFKBPEvI1FXTXXhMJ8HUumi+1bUjd73jf\n" +
                "luhhyubod8VIG4P8PTqLbOLu37p4T+qChkWHJr0vC1wCZ8nxI5ECggEBAOl4fo9o\n" +
                "M+j3GkcM/jEJWQ0vqKTArXO/DZ+fGzJ8Y6mom3oGHE5eujUcyUpequGju1uoquGX\n" +
                "BxrOPQMq8FtVSr+tUTIy2O1YUe5omc1nRf1PtAMP+J74Xp3LRxdGukYCbRRQVewH\n" +
                "jmgIDAyGO+3HSLOqL3KNBDZCRmJp6zMNqX4abRXVbL4/lhhUpSzlxRqmuE8Id80F\n" +
                "BTyL1CzvDNjrjYa6CgInPT/dHiVrnNAdW50tKhYimrwjB5/qbjoanEydgkyGTbFd\n" +
                "5z8tMLVFJ7w4tPHkmsYjvI0p0I9FPJD2GIfrpvFPr/uVrQ6+d8roH3blAfGns4QW\n" +
                "8hIARNSqoadd9WkCggEBAMokRWO8z7k+u97F39dbTqEXyHjWUPu5f0iIolb9N1dn\n" +
                "kjMJ7qq1A10WYeZVb/wnlpR2r+Q3zUxZqZigaCOas2ksHF/Ujruu3QrYkR/r1yXB\n" +
                "MUDxaeo50aZp+HcmAE7eKySBw/TJMn4yvCuX45sIN82zQWpeRekcSFAjOQfRV2HW\n" +
                "P4ZIDysn7DsGf+vm8cAsW6Quq3PFTBGonS7RYMlhknU1/RfH4g24n88cler8oTYM\n" +
                "s/jp5hpjkxJ0Unb+8pV0OXLnWVSmMLUVSVlHOl4CCKCTNQvKf28upwhB1pPWej6i\n" +
                "NMqdY+fIt+Fd985V0Ir+B21ShVDed5c72bpB06n9WdcCggEAG+Epq9JTsJQhbS6e\n" +
                "BBkLq0lvqAziKZo89Dy5sLOt6wqZVl74bltdfQ4s81aOrVcx/mYL0diJHqhWHNS5\n" +
                "0w5CWNVHhukPgngzgHa5NxAICZHE+0Ci/cjG86zclmj5wXZ0tCJLwF2+oamkVrKI\n" +
                "4YIUqm++Lr2sLRaI9SOU1InjHY3mTN8plyZctBcXil79xIr4I2ftdmwNDgfclGkP\n" +
                "ba/jPJ1mqI8q/z9WZD2PgkKfOAu2pOII/EJqnKwP8ZxP4c5FSwIWsQF3pdGtqVfS\n" +
                "wOU8pk4YNWT7FRhTMWihLOZWU5TOYK6VY0OiYMpZ378MUtRSARt3kmRzD7c8gPDH\n" +
                "UQclUQKCAQB4n9RYhB9g57KsaV/93xq4vrx+f0WsMTFnU0Gsr0YK/l8b3d1yOLpd\n" +
                "HjIlhO5ihi0xQvILOdFkskymK3J5bKOLKytzdCAIl3yIMFvJtK6adQKzQlx1zTLy\n" +
                "H2KJlz+v0JvmGRmaRUXAUP5A9U55ARprwYBTvRXy2VG9oIczxxRh6bvWocGLezNY\n" +
                "tbQ4TYQNrWqyOrdNSnruPrQtb/xVr8f58dGqEzkt/vI+YUyFAWQiIMp0yv7o2Gq3\n" +
                "JHrhT5nq3YQ6sRt5jAKczKsMf5iw6H3FdJK/CoOpESnTn5YwelhQb/MYxXsMoZY5\n" +
                "Ah4SHttnVdeQwSGU9Gxg7vIqV4W7dtfZAoIBAEeRfFhJUAX1re+aNt7r8e2hnpzO\n" +
                "jVHRs6mWz8cghd0SCxThkl31J0juHnR/w5XyggHeHjU+BXOQcmE3EGU+ca3X5UxN\n" +
                "W+Awlv28LtotNDoGXSZxYhzsFLpB6rKcq48C2nKrjdX53CTwx+XwKGou0NWSTIUv\n" +
                "qx4eVxIamH/Eox7OLzNxvME0lgvlvVWmj4M7KmyA4W0V1vtulMGYAPFg3ZBuhBzj\n" +
                "e+tIlgoRqeOF3ntBM5btAgHik9PVocBQEeRpQU2EvqN3kgh+97Nkg7/ONDsqf5Uc\n" +
                "cTVD5cep8RnqKwcdZ6HgsdX2uQCX3lkpzGuBWpCJUZl0VWIgL2CUl1KH03E=\n" +
                "-----END RSA PRIVATE KEY-----";

        String cert = "-----BEGIN CERTIFICATE-----\n" +
                "MIIFPjCCAyYCCQDOpffyFp9V4DANBgkqhkiG9w0BAQUFADBhMQswCQYDVQQGEwJG\n" +
                "UjENMAsGA1UEBwwETHlvbjEQMA4GA1UECgwHSHVyZW5jZTENMAsGA1UECwwESUlv\n" +
                "VDEiMCAGCSqGSIb3DQEJARYTbm9yZXBseUBodXJlbmNlLmNvbTAeFw0xODA2Mjcx\n" +
                "NDUyNDFaFw0xOTA2MjgxNDUyNDFaMGExCzAJBgNVBAYTAkZSMQ0wCwYDVQQHDARM\n" +
                "eW9uMRAwDgYDVQQKDAdIdXJlbmNlMQ0wCwYDVQQLDARJSW9UMSIwIAYJKoZIhvcN\n" +
                "AQkBFhNub3JlcGx5QGh1cmVuY2UuY29tMIICIjANBgkqhkiG9w0BAQEFAAOCAg8A\n" +
                "MIICCgKCAgEAuFooF2RLI/v3z01grK6UDpi3efJO2EI/w93gD/61r486LB3JWk2g\n" +
                "ffNgJZEpaQJTE/gcIN4faPIcAn5/ZvzXZ+LgzpAOByq/Xo6zjp/QqZhsBjyU2+Dr\n" +
                "SU2dga8CtA9KLkkRtM2z4w1eMeTDq8D0IvOPPgxPSmyY1Iju7LtoYRLhSgg1zNgO\n" +
                "skkOwJPgpl89dwVYDSQqTeUhVyWNH0hmxT8VxgG7U+VRBPoIr7NLgNBF5VMBVJPE\n" +
                "qsAmeG3fMmjxYgjOeXRQDLWHVboKv1OnTbYvfDqH8V0S8MrYOqp4afu4ogcvOj8k\n" +
                "fQ0eeqiSMy1s2vssupzhiXHwAViLAPyMBaxGIai157rBXVf+c8FJG+Vklmb3WYs5\n" +
                "uMwRsXNc86+RfoWSxBzKvPqx0aA64tsFZMPD/59Dk2Rj2xVs1gD64X38ucMJ16CG\n" +
                "/1BDlkwmfmWPpKeB/ALFmQuIXZFHMUnzkI8AJiWP6hjo3yGnHFwBe36OHyHGYrCW\n" +
                "X/km/rrmLV4AG04neSZ1zxeKMTleLTUg5cdW8NQzOywe0m1NIG9RZ99WdTsAqkyq\n" +
                "J1tnwbup6j2UVvszz6O7uRNso260Xn9GKPZRKUJHW4aPTPPQOYgRvBhqSlhwX1Au\n" +
                "vPREdzvIfnrqj4jmhWS7MMtFK3+oFC1wI8kBjClAaQ0yNDfeXBP/nC8CAwEAATAN\n" +
                "BgkqhkiG9w0BAQUFAAOCAgEAjifK9q2Lxpp/oy160ZOz8TVgUapoSssB9ExXyB4Y\n" +
                "5y6jSOEgE/TNNByriwypWItmVkW/hPtZBgG7h2FKOYImeYvmsr4El+I/TxX1r4lB\n" +
                "F6Y/EQaZGTSzBlsu0hXyk1xFmoQNX/yn8SWXLW2Bslbiu6F/c+QlOLOSGQ23Vi/O\n" +
                "513As6ZXuNnbmak4zUV5n6cvwQTpkX53IACCdh09hKjYXsEknv5G1K7VGT1r4/fx\n" +
                "V+qO7DHkyVHGkp56vf/atOczPv/SbVOfTGpEMXsbgJH+Ll8xjDFybV3sUcBXnrB5\n" +
                "IWJOIRRrlj/xynuibR6RgVMK5rhLM1wlIlKDXACsnOuUz2fgaL3nITuw/AoYObrB\n" +
                "uglQNhXBXZohgWqcvYuhIlFxYLjS99PqiV+U5vowpp2V6xKv5mbHIoffwrgXdKjg\n" +
                "AhDkLXRfgH1sIwBMazBzFc5AIhUJ4IwbPFc7F7kYdHYcIUGJgiln2wExNqQ2Xebz\n" +
                "gryyQGZlyCykkPcQzPAzMPSV5H73vpSvcjLD0lWi314ahhUb6WHpU2fGNRgV6lfp\n" +
                "JE7pvGhBY5Df84JI3v8jl19lWINjHvbRNrDmciM9M2vpmo6VaToh8uoabBzA+t/d\n" +
                "DaqOlD8Ek7cPqnFFE2vLhTRJaFHBUcsBV+GGPizsbcEtnk3GGhyOVsJDeQ3m4VXf\n" +
                "jdI=\n" +
                "-----END CERTIFICATE-----";

        Method method = OpcUaSourceTask.class.getDeclaredMethod("decodeCredentials", String.class, String.class);
        method.setAccessible(true);
        OpcUaSourceTask task = new OpcUaSourceTask();
        X509Credentials ret = (X509Credentials) method.invoke(task, cert, key);
        System.out.println(ret);
        Assert.assertNotNull(ret);
        Assert.assertNotNull(ret.getCertificate());
        Assert.assertNotNull(ret.getPrivateKey());

    }

}