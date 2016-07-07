package com.hurence.logisland.utils.avro.eventgenerator;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


/*
 * The interface to generate random values.
 */
public interface RandomDataGenerator {
  public int getNextInt();
  public int getNextInt(int min, int max);
  public String getNextString();
  public String getNextString(int min, int max);
  public double getNextDouble();
  public float getNextFloat();
  public long getNextLong();
  public boolean getNextBoolean();
  public  byte[] getNextBytes(int maxBytesLength);
}
