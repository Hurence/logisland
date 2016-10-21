/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.tmp;

/*
    Span.java

    NOTE: This file is a generated file.
          Do not modify it by hand!
*/
// add your custom import statements here
/********************************************************
 * 
 * Span - range manipulation class for Signalgo library
 *
 * Author:        Vadim Schetinkin
 *
 * (c) by         Vadim Schetinkin
 *                www.chat.ru/~vadim2000 
 *                esraobe@ufanet.ru
 *
 ********************************************************* */

public class Span extends Object
{
    // add your data members here
    public int start;
    public int end;    

    // ***************************************************
    // * CONSTRUCTORS methods
    // * date: 12/07/99
    // ***************************************************

	// Span's constructor
	public Span(){
	} // < constructor

	public Span(int s, int e){
        start = s;
        end = e;
	} // < constructor

	public Span(Span s){
            start = s.start;
            end = s.end;
	} // < constructor

	public int getStart(){
        return start;
	} // < getStart

	public int getEnd(){
        return end;
	} // < getEnd

	public void setStart(int s){
        start = s;
	} // < setStart

	public void setEnd(int e){
        end = e;
	} // < setEnd

	public void setSpan(int s, int e){
        start = s;        
        end = e;
	} // < setSpan

	public void setSpan(Span S){
        start = S.start;        
        end = S.end;
	} // < setSpan

	public int getDistance(){
        return ( end - start );
	} // < getDistance

	public int getLength(){
        return ( end - start + 1 );
	} // < getLength

	public boolean Above(int i){
        return ( i > end ) ? true:false;
	} // < aboveSpan

	public boolean Below(int i){
        return ( i < start ) ? true:false;
	} // < belowSpan

	public boolean Inside(int i){
        return ( i >= start && i <= end ) ? true:false;
	} // < insideSpan

	public boolean Equals(Span s){
        return ( s.start == start && s.end == end ) ? true:false;
	} // < Equals

	public static boolean Equals(Span a, Span b){
        return ( a.start == b.start && a.end == b.end ) ? true:false;
	} // < Equals

	public static boolean Inside(int i, Span S){
        return ( i >= S.start && i <= S.end ) ? true:false;
	} // < insideSpan

	public boolean inSpan(int i){
        return ( i <= end ) ? true:false;
	} // < inSpan

	public boolean insideEdges(int left, int right){
        return ( start >= left && end <= right ) ? true:false;
	} // < insideEdges

	public boolean insideEdges(Span S){
        return ( start >= S.start && end <= S.end ) ? true:false;
	} // < insideEdges

    public void setEndToStart() {
        end = start;
    } // < setEndToStart

    public int getDistance_End(int p){
        // > 
        return ( end - p );
    } // < getDistance_End

    public int getDistance_Start(int p){
        // > returns distance between p and start
        return ( p - start );
    } // < getDistance_Start
}

