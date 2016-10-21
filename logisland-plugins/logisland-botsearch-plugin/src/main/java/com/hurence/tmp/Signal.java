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
    Signal.java

    NOTE: This file is a generated file.
          Do not modify it by hand!
*/
/********************************************************
 * 
 * Signal - base class for Signalgo library
 *
 * Author:        Vadim Schetinkin
 *
 * (c) by         Vadim Schetinkin
 *                www.chat.ru/~vadim2000 
 *                esraobe@ufanet.ru
 *
 * version 0.5a 22.07.99
 ********************************************************* */


// add your custom import statements here
import java.lang.System;
import java.lang.Math;

public class Signal extends Object
{
    // this arrays holds signal itself within
    private float[] _s;    // signal as float values 
    private short[] _b;    // 16-bit values

    // current position in this signal 
    private int   _Pos;
    // selection start & end point (markers)
    private Span  Sel;
    // sampling rate of the signal
    private int  _rate;
    // virtual zero to use instead 0.0f in such
    // situations as division, etc
    public final float virtualZero = 0.01f;
    public final int defRate = 44100;    

    // ***************************************************
    // * CONSTRUCTORS
    // ***************************************************

	// Signal's constructor
	public Signal(){
        _s = new float[ defRate ]; // 1 sec in 44100
        _b = new short[ defRate ];        

        _rate = defRate ;
        selectAll();
        Clear();
	} // < constructor

	public Signal(int n){
        // n - length of iternal array to create
        _s = new float[n];
        _b = new short[n];        

        _rate = defRate ;
        selectAll();
        Clear();
	} // < constructor

    public Signal(Signal someSignal){
        _s = someSignal._s;
        _b = someSignal._b;        

        Sel.setSpan(someSignal.Sel);        
        _rate = someSignal._rate; 
        toSelStart();        
    } // < Signal

    // ***************************************************
    // * PROPERTIES methods
    // * date: 12/07/99
    // ***************************************************

    // --------- _rate -----------

    public void setRate(int Rate) {
        _rate = Rate;
    } // < setRate

    public int getRate() {
        return _rate;
    } // < getRate

    // -----------  _Pos -------------------

    public void setPos(int Pos) {
        if ( ( _Pos < _s.length) & ( _Pos >= 0)) {
            _Pos = Pos;            
        }
    } // < setPos

    public int getPos() {
        return _Pos;
    } // < getPos

    // ------------- Sel -----------------

    public void setSel(Span s) {
//        Sel,setSpan(s);
        Sel = s;
    } // < setSel

    public Span getSel() {
        return Sel;
    } // < getPos

    // ------------------------------

    public float getVirtualZero() {
        return virtualZero;
    } // < getVirtualZero

    // ***************************************************
    // * ACCESS methods
    // * date: 28/06/99
    // ***************************************************

    // ------- setting methods -------------    
    public void setIt(float v) {
        if ( ( _Pos < _s.length) & ( _Pos >= 0)) {
           _s [_Pos] = v;   
           _b [_Pos] = (short)v;                
        }
    } // < setIt 

    public void setIt(int v) {
        if ( ( _Pos < _s.length) & ( _Pos >= 0)) {        
           _s [_Pos] = (float)v;   
           _b [_Pos] = (short)v;       
        }
    } // < setIt 

    public void setItAt(int p, float v) {
        if ( (p < _s.length) & (p >= 0)) {        
           _s [p] = v;   
           _b [p] = (short)v;
        }
    } // < setItAt

    public void setItAddOffset (int offset, float v) {
        int p = _Pos + offset;
        setItAt ( p, v );
    } // < setItAddOffset

    public void setItSubOffset (int offset, float v) {
        int p = _Pos - offset;
        setItAt ( p, v );
    } // < setItSubOffset

    public void setItIndex (int x, float v) {
        // relatively to Sel
        int p = Sel.getStart() + x;
        setItAt ( p, v );        
    } // < setItIndex 

    public void setItShort(float v) {
        if ( ( _Pos < _s.length) & ( _Pos >= 0)) {                
           _b [_Pos] = (short)v;
        }
    } // < setItShort 

    public void setItShortAt(int p, float v) {
        if ( (p < _s.length) & (p >= 0) ) {        
           _b [p] = (short)v;
        }
    } // < setItShort 

    // ------- getting methods -------------
    public float getIt () {
        if ( ( _Pos < _s.length) & ( _Pos >= 0)) {
            return _s [ _Pos ];
        } else {
            return 0.0f;
        }
    } // < getIt 

    public float getItAt (int p) {
        if ( ( p < _s.length) & ( p >= 0)) {
            return _s [ p ];
        } else {
            return 0.0f;
        }
    } // < getItAt

    public float getItAddOffset (int offset) {
        int p = _Pos + offset;
        return getItAt ( p );
    } // < getItAddOffset

    public float getItSubOffset (int offset) {
        int p = _Pos - offset;
        return getItAt ( p );        
    } // < getItSubOffset

    public float getItIndex (int x) {
        // relatively to Sel
        int p = Sel.getStart() + x;
        return getItAt ( p );        
    } // < getItIndex 

    public short getItAsShort () {
        if ( ( _Pos < _s.length) & ( _Pos >= 0) ) {
            return (short)_s[ _Pos ];
        } else {
            return 0;
        }
    } // < getItAsShort

    public int getItAsInt () {
        if ( ( _Pos < _s.length) & ( _Pos >= 0) ) {
            return (int)_s[ _Pos ];
        } else {
            return 0;
        }

    } // < getItAsInt 

    // ------------ conditions checking --------------
    // is greater then ---------------------------
    public boolean isG(float v){
        return ( getIt() > v ) ? true:false;
	} // < isG

    public boolean isG_At(int p, float v){
        return ( getItAt(p) > v ) ? true:false;
	} // < isG_At
    
    public boolean isG_Index(int x, float v){
        return ( getItIndex(x) > v ) ? true:false;
	} // < isG_Index

    public boolean isG_AddOffset(int x, float v){
        return ( getItAddOffset(x) > v ) ? true:false;
	} // < isG_AddOffset

    public boolean isG_SubOffset(int x, float v){
        return ( getItSubOffset(x) > v ) ? true:false;
	} // < isG_SubOffset

    public static boolean isG(float a, float v){
        return ( a > v ) ? true:false;
	} // < isG

    // is greater or equal then --------------------
    public boolean isGE(float v){
        return ( getIt() >= v ) ? true:false;
	} // < isGE

    public boolean isGE_At(int p, float v){
        return ( getItAt(p) >= v ) ? true:false;
	} // < isGE_At
    
    public boolean isGE_Index(int x, float v){
        return ( getItIndex(x) >= v ) ? true:false;
	} // < isGE_Index

    public boolean isGE_AddOffset(int x, float v){
        return ( getItAddOffset(x) >= v ) ? true:false;
	} // < isGE_AddOffset

    public boolean isGE_SubOffset(int x, float v){
        return ( getItSubOffset(x) >= v ) ? true:false;
	} // < isGE_SubOffset

    public static boolean isGE(float a, float v){
        return ( a >= v ) ? true:false;
	} // < isGE

    // is less then ------------------------
    public boolean isL(float v){
        return ( getIt() < v ) ? true:false;
	} // < isL

    public boolean isL_At(int p, float v){
        return ( getItAt(p) < v ) ? true:false;
	} // < isL_At
    
    public boolean isL_Index(int x, float v){
        return ( getItIndex(x) < v ) ? true:false;
	} // < isL_Index

    public boolean isL_AddOffset(int x, float v){
        return ( getItAddOffset(x) < v ) ? true:false;
	} // < isL_AddOffset

    public boolean isL_SubOffset(int x, float v){
        return ( getItSubOffset(x) < v ) ? true:false;
	} // < isL_SubOffset

    public static boolean isL(float a, float v){
        return ( a < v ) ? true:false;
	} // < isL

    // is less or equal then --------------------
    public boolean isLE(float v){
        return ( getIt() <= v ) ? true:false;
	} // < isLE

    public boolean isLE_At(int p, float v){
        return ( getItAt(p) <= v ) ? true:false;
	} // < isLE_At
    
    public boolean isLE_Index(int x, float v){
        return ( getItIndex(x) <= v ) ? true:false;
	} // < isLE_Index

    public boolean isLE_AddOffset(int x, float v){
        return ( getItAddOffset(x) <= v ) ? true:false;
	} // < isLE_AddOffset

    public boolean isLE_SubOffset(int x, float v){
        return ( getItSubOffset(x) <= v ) ? true:false;
	} // < isLE_SubOffset

    public static boolean isLE(float a, float v){
        return ( a <= v ) ? true:false;
	} // < isLE

    // is equal then --------------------
    public boolean isE(float v){
        return ( getIt() == v ) ? true:false;
	} // < isE

    public boolean isE_At(int p, float v){
        return ( getItAt(p) == v ) ? true:false;
	} // < isE_At
    
    public boolean isE_Index(int x, float v){
        return ( getItIndex(x) == v ) ? true:false;
	} // < isE_Index

    public boolean isE_AddOffset(int x, float v){
        return ( getItAddOffset(x) == v ) ? true:false;
	} // < isE_AddOffset

    public boolean isE_SubOffset(int x, float v){
        return ( getItSubOffset(x) == v ) ? true:false;
	} // < isE_SubOffset

    public static boolean isE(float a, float v){
        return ( a == v ) ? true:false;
	} // < isE

    // is not equal then --------------------
    public boolean isNE(float v){
        return ( getIt() != v ) ? true:false;
	} // < isNE

    public boolean isNE_At(int p, float v){
        return ( getItAt(p) != v ) ? true:false;
	} // < isNE_At
    
    public boolean isNE_Index(int x, float v){
        return ( getItIndex(x) != v ) ? true:false;
	} // < isNE_Index

    public boolean isNE_AddOffset(int x, float v){
        return ( getItAddOffset(x) != v ) ? true:false;
	} // < isNE_AddOffset

    public boolean isNE_SubOffset(int x, float v){
        return ( getItSubOffset(x) != v ) ? true:false;
	} // < isNE_SubOffset

    public static boolean isNE(float a, float v){
        return ( a != v ) ? true:false;
	} // < isNE

    // ------------ absolut values conditions --------
    // is greater then ---------------------------
    public boolean isAbsG(float v){
        return ( Math.abs( getIt() ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG

    public boolean isAbsG_At(int p, float v){
        return ( Math.abs( getItAt(p) ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG_At
    
    public boolean isAbsG_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG_Index

    public boolean isAbsG_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG_AddOffset

    public boolean isAbsG_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG_SubOffset

    public static boolean isAbsG(float a, float v){
        return ( Math.abs( a ) > Math.abs( v ) ) ? true:false;
	} // < isAbsG

    // is greater or equal then --------------------
    public boolean isAbsGE(float v){
        return ( Math.abs( getIt() ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE

    public boolean isAbsGE_At(int p, float v){
        return ( Math.abs( getItAt(p) ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE_At
    
    public boolean isAbsGE_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE_Index

    public boolean isAbsGE_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE_AddOffset

    public boolean isAbsGE_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE_SubOffset

    public static boolean isAbsGE(float a, float v){
        return ( Math.abs( a ) >= Math.abs( v ) ) ? true:false;
	} // < isAbsGE


    // is less then ------------------------
    public boolean isAbsL(float v){
        return ( Math.abs( getIt() ) < Math.abs( v ) ) ? true:false;
    } // < isAbsL

    public boolean isAbsL_At(int p, float v){
        return ( Math.abs( getItAt(p) ) < Math.abs( v ) ) ? true:false;
	} // < isAbsL_At
    
    public boolean isAbsL_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) < Math.abs( v ) ) ? true:false;
	} // < isAbsL_Index

    public boolean isAbsL_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) < Math.abs( v ) ) ? true:false;
	} // < isAbsL_AddOffset

    public boolean isAbsL_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) < Math.abs( v ) ) ? true:false;
	} // < isAbsL_SubOffset

    public static boolean isAbsL(float a, float v){
        return ( Math.abs( a ) < Math.abs( v ) ) ? true:false;
	} // < isAbsL

    // is less or equal then --------------------
    public boolean isAbsLE(float v){
        return ( Math.abs( getIt() ) <= Math.abs( v ) ) ? true:false;
	} // < isAbsLE

    public boolean isAbsLE_At(int p, float v){
        return ( Math.abs( getItAt(p) ) <= Math.abs( v )) ? true:false;
	} // < isAbsLE_At
    
    public boolean isAbsLE_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) <= Math.abs( v ) ) ? true:false;
	} // < isAbsLE_Index

    public boolean isAbsLE_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) <= Math.abs( v ) ) ? true:false;
	} // < isAbsLE_AddOffset

    public boolean isAbsLE_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) <= Math.abs( v ) ) ? true:false;
	} // < isAbsLE_SubOffset

    public static boolean isAbsLE(float a, float v){
        return ( Math.abs( a ) <= Math.abs( v ) ) ? true:false;
	} // < isAbsLE

    // is equal then --------------------
    public boolean isAbsE(float v){
        return ( Math.abs( getIt() ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE

    public boolean isAbsE_At(int p, float v){
        return ( Math.abs( getItAt(p) ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE_At
    
    public boolean isAbsE_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE_Index

    public boolean isAbsE_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE_AddOffset

    public boolean isAbsE_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE_SubOffset

    public static boolean isAbsE(float a, float v){
        return ( Math.abs( a ) == Math.abs( v ) ) ? true:false;
	} // < isAbsE

    // is not equal then --------------------
    public boolean isAbsNE(float v){
        return ( Math.abs( getIt() ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE

    public boolean isAbsNE_At(int p, float v){
        return ( Math.abs( getItAt(p) ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE_At
    
    public boolean isAbsNE_Index(int x, float v){
        return ( Math.abs( getItIndex(x) ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE_Index

    public boolean isAbsNE_AddOffset(int x, float v){
        return ( Math.abs( getItAddOffset(x) ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE_AddOffset

    public boolean isAbsNE_SubOffset(int x, float v){
        return ( Math.abs( getItSubOffset(x) ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE_SubOffset

    public static boolean isAbsNE(float a, float v){
        return ( Math.abs( a ) != Math.abs( v ) ) ? true:false;
	} // < isAbsNE
    
    // -------whole signal access --------------------
    public void setSignal(float[] v){
        _s = v;
        short[] b = new short[ v.length ];
        for (int i=0; i<v.length;i++){
            b[i] = (short) _s[i];            
        }
        _b = b;
    } //setSignal 

    public float[] getSignal(){
        return _s;
    } //getSignal

    // --------- access vectors within signal ------- 
    public float[] getItNum (int sp, int n) {
        // > sp - start position
        // > n - # of samples to return

        float[] y = new float [n];

        for (int i=0; i<n; i++){
            int p = i + sp;            
            y[i] = getItAt( p );                
        }
        return y;

    } // < getItNum

    // ***************************************************
    // * ITERATOR methods
    // * date: 28/06/99
    // ***************************************************

    public void setItForward(float v) {
        // > forward iterator
       setIt(v);
       nextPos();
    } // < setItForward

    public void setItBackward(float v) {
        // > backward iterator
       setIt(v);  
       prevPos();
    } // < setItBackward 

    public float getItForward() {
       // > forward iterator
       float tmp = getIt();
       nextPos();
       return tmp;
    } // < getItForward

    public float getItBackward() {
        // > backward iterator
       float tmp = getIt();        
       prevPos();
       return tmp;       
    } // < getItBackward 

    // ----------selection mainpulation --------------    
    public boolean inSel() {
        // > within selection check (inSel)
        return Sel.inSpan( _Pos );
    } // < inSel

    //--------- position altering --------------
    public void nextPos() {
        _Pos++;
        if ( _Pos > Sel.getEnd() ) { // FIXME!!! there's doubt
            _Pos = Sel.getEnd();
        }
    } // < nextPos

    public void nextPos(int Step) {
        _Pos += Step;
        if ( _Pos > Sel.getEnd() ) { // FIXME!!! there's doubt
            _Pos = Sel.getEnd();
        }
    } // < nextPos

    // -------- fast forward & back within selection ----
    public void toSelStart() {
        _Pos = Sel.getStart();
    } // < toSelStart

    public void toSelEnd() {
        _Pos = Sel.getEnd();
    } // < toSelEnd

    // ------------------------------
    public void prevPos() {
        if ( _Pos < Sel.getStart() ) { // FIXME!!! there's doubt
            _Pos--;
        }      
    } // < prevPos

    public void prevPos(int Step) {
        _Pos -= Step;
        if ( _Pos < Sel.getStart() ) { // FIXME!!! there's doubt
            _Pos = Sel.getStart();
        }      
    } // < prevPos

    // ---------- LENGTH CALCULATIONS ----------------

    public int getDistanceToEnd(){
        // > 
        return _s.length - _Pos - 1 ;
    } // < getDistanceToEnd

    public int getDistanceToStart(){
        // > returns distance between _Pos and _s[0]
        return _Pos;
    } // < getDistanceToStart

    public int getSelectionLength(){
        // > returns distance between _from and _thru
        return (Sel.getEnd() - Sel.getStart() + 1 );
    } // < getSelectionLength


    // ***************************************************
    // * CHECK methods
    // * date: 28/06/99
    // ***************************************************

    public void checkLeftSelBoundary () {
       if ( _Pos < Sel.getStart() ) { _Pos = Sel.getStart() ; } 
    } // < checkLeftSelBoundary 

    public void checkRightSelBoundary () {
       if (_Pos > Sel.getEnd() ) { _Pos = Sel.getEnd(); }   
    } // < checkRightSelBoundary 

    public void FixPosInSelection() {
       if (_Pos > Sel.getEnd()) { _Pos = Sel.getEnd(); }   
       if (_Pos < Sel.getStart()) { _Pos = Sel.getStart(); }          
    } // < checkRightSelBoundary 

    // ***************************************************
    // * UTILITIES methods
    // * date: 02/07/99
    // ***************************************************

    // -------- sign checking ------------    
    public boolean isPositive() {
        // > is current value of sample positive (it means >=0)
        return ( getIt() >= 0.0f ) ? true:false;
    } // < isPositive

    public static boolean isPositive(float v) {
        // > is v positive (it means >=0)
        return ( v >= 0.0f ) ? true:false;
    } // < isPositive

    public boolean isPositiveAt(int p) {
        // > is value of sample positive (it means >=0)
        return ( getItAt(p) >= 0.0f ) ? true:false;
    } // < isPositive

    public boolean isPositiveIndex(int x) {
        // > is value of sample positive (it means >=0)
        return ( getItIndex(x) >= 0.0f ) ? true:false;
    } // < isPositiveIndex

    public float FetchT_B(float t, float b) {
        // > returns top/bottom signal value
        if ( isPositive() ){
            return t;
        } else {
            return b;
        }
    } // < FetchT_B

    public static float FetchT_B(float v, float t, float b) {
        // > returns top/bottom signal value
        if ( isPositive(v) ){
            return t;
        } else {
            return b;
        }
    } // < FetchT_B

    public float FetchAtT_B(int p, float t, float b) {
        // > returns top ot bottom signal value
        if ( isPositiveAt(p) ){
            return t;
        } else {
            return b;
        }
    } // < FetchAtT_B

    public float FetchIndexT_B(int x, float t, float b) {
        // > returns top ot bottom signal value
        if ( isPositiveIndex(x) ){
            return t;
        } else {
            return b;
        }
    } // < FetchIndexT_B

    // -------- arrays manipulation ------------
    public boolean arraysAreEqual (float[] a, float[] b) {
        if (a.length == b.length) {
            return true;
        } else {
            return false;
        }
    } // < arraysAreEqual  

    public float[] ExpandArray (float[] a) {
        // if a[].length < Sel.getLength() -> expand

        if ( (a.length < Sel.getLength() ) | (a == null) ) {
            Signal y = new Signal( Sel.getLength() );
            float[] v = y.getSignal();
            System.arraycopy(a, 0, v, 0, a.length);
            return v;
        } else {
            return a;
        }
    } // < ExpandArray  

    public static float[] ExpandArray (float[] a, int n) {
        // if a[].length < n -> expand

        if ( (a.length < n ) | (a == null) ) {
            Signal y = new Signal( n );
            float[] v = y.getSignal();
            System.arraycopy(a, 0, v, 0, a.length);
            return v;
        } else {
            return a;
        }
    } // < ExpandArray  

    public boolean RatesAreEqual (Signal a, Signal b) {
        if (a._rate == b._rate) {
            return true;
        } else {
            return false;
        }
    } // < RatesAreEqual

    // ***************************************************
    // * INITIALIZATION methods
    // * date: 28/06/99
    // ***************************************************

    public void SetTB(float t, float b) {
        // > sets to t top & to b bottom samples within selection

        toSelStart(); 
        while ( inSel() ) { 
            setItForward( FetchT_B(t, b) );            
        }
    } // < SetTB

    public void Set(float v) {
        // > sets to Value all samples within selection
        SetTB(v, v);        
    } // < Set

    public void SetT(float t) {
        // > sets to Value all samples within selection
        toSelStart(); 
        while (inSel()) { 
            if ( isPositive() ){
                setItForward( t );
            }
        }
    } // < SetT

    public void SetB(float b) {
        // > sets to Value all samples within selection

        toSelStart(); 
        while (inSel()) { 
            if ( !isPositive() ){
                setItForward( b );                
            }
        }
    } // < SetB

    // ----------- static vector return -------------------
    public static float[] vSetTB(float[] v, float t, float b) {
        // > sets to t top & to b bottom samples in array v

        int n = v.length;
        for (int i=0; i<n; i++){
            v[i] = Signal.FetchT_B( v[i], t, b );
        }
        return v;
    } // < vSetTB

    public static float[] vSet(float[] v, float s) {
        // > sets to Value all samples in array v
        int n = v.length;
        for (int i=0; i<n; i++){
            v[i] = s;
        }
        return v;

    } // < vSet

    public static float[] vSetT(float[] v, float t) {
        // > sets to Value all samples in array v
        int n = v.length;
        for (int i=0; i<n; i++){
            if ( Signal.isPositive(t) ){
                v[i] = t;
            }
        }
        return v;
    } // < vSetT

    public static float[] vSetB(float[] v, float b) {
        // > sets to Value all samples in array v
        int n = v.length;
        for (int i=0; i<n; i++){
            if ( !Signal.isPositive(b) ){
                v[i] = b;
            }
        }
        return v;
    } // < vSetB

    public void Clear() {
        // > sets all samples to 0.0f within selection

        Set(0.0f);
    } // < Clear

    public void ClearT() {
        // > sets all top samples to 0.0f within selection

        SetT(0.0f);
    } // < ClearT

    public void ClearB() {
        // > sets all bottom samples to 0.0f within selection

        SetB(0.0f);
    } // < ClearB

    public void Set(float[] v) {
        // > sets all samples within selection to Vector values
        // 
        int i=0;
        v = ExpandArray(v);

        toSelStart(); 
        while (inSel()) { setItForward(v[i++]); };
    } // < Set

    // ***************************************************
    // * SELECTION methods
    // * date: 28/06/99
    // ***************************************************

    public void selectAll() {
        // > selects all samples
        //        
        Sel.setSpan( 0, _s.length-1 );               
        toSelStart();
    } // < selectAll

    // ***************************************************
    // * ARITHMETIC methods
    // * date: 02/07/99
    // ***************************************************

    // ------ ADDITION METHODS -------------
    // signal + values methods
    public void AddTB(float t, float b) {
        // > adds different values to top and bottom wave

        toSelStart(); 
        while (inSel()){
            setItForward( getIt() + FetchT_B(t, b) );             
        }
    } // < AddTB

    public void Add(float v) {
        // > adds a value to each sample

        AddTB(v, v);
    } // < Add

    public void AddT(float t) {
        // > adds a value to each sample of top wave

        AddTB(t, 0.0f);        
    } // < AddT

    public void AddB(float b) {
        // > adds a value to each sample of bottom wave

        AddTB(0.0f, b);        
    } // < AddB

    // ---------- vector arithmetic ------------------
    // siganl + vectors values
    public void AddTB(float[] t, float[] b) {
        // > adds different values from vectors to top and bottom wave
        t = ExpandArray(t);
        b = ExpandArray(b);        

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setIt( getItIndex(i) + FetchT_B(t[i], b[i]) );            
        }
    } // < AddTB

    public void Add(float[] v) {
        // > adds vectors 
        int i=0;
        v = ExpandArray(v);        
        toSelStart(); 
        while (inSel()){
            setItForward( getIt() + v[i++] );
        }
    } // < Add

    public void AddT( float[] t ) {
        // > adds a value to each sample of top wave
        float[] b = null;
        AddTB( t, b );        
    } // < AddT

    public void AddB( float[] b ) {
        // > adds a value to each sample of bottom wave
        float[] t = null;
        AddTB( t, b );        
    } // < AddB

    // ---------- vector return ------------------
    // vector = siganl + values
    public float[] vAddTB(float t, float b) {
        // > adds different values to top and bottom wave
        int e = Sel.getLength();        
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) + FetchT_B( t, b );
        }
        return y;        
    } // < vAddTB

    public float[] vAdd(float v) {
        // > adds a value to each sample
        return vAddTB(v, v);
    } // < vAdd

    public float[] vAddT(float t) {
        // > adds a value to each sample of top wave
        return vAddTB(t, 0.0f);        
    } // < vAddT

    public float[] vAddB(float b) {
        // > adds a value to each sample of bottom wave
        return vAddTB(0.0f, b);        
    } // < vAddB

    // -------------- vector in & out ---------------
    // vector = siganl + vector values
    public float[] vAddTB(float[] t, float[] b) {
        // > adds different values to top and bottom wave
        // > and returns vector

        int e = Sel.getLength();
        float[] y = new float[ e ];

        t = ExpandArray( t );
        b = ExpandArray( b );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) + FetchT_B( t[i], b[i] );
        }
        return y;
    } // < vAddTB

    public float[] vAdd(float[] v) {
        // > adds vectors 

        int e = Sel.getLength();
        float[] y = new float[ e ];

        v = ExpandArray( v );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) + v[i];
        }
        return y;        
    } // < vAdd

    public float[] vAddT(float[] t) {
        // > adds a value to each sample of top wave
        // > and returns vector
        float[] b = null; 

        return vAddTB(t, b);        
    } // < vAddT

    public float[] vAddB(float[] b) {
        // > adds a value to each sample of bottom wave
        // > and returns vector        
        float[] t = null; 

        return vAddTB(t, b);        
    } // < vAddB

    // ----------- static vector return methods --------------------
    // vector = vector + values
    public static float[] vAddTB(float[] v, float t, float b) {
        // > adds different values to top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = v[i] + Signal.FetchT_B(v[i], t, b );
        }
        return y;
    } // < vAddTB

    public static float[] vAdd(float[] a, float v) {
        // > adds a value to each sample

        return vAddTB(a, v, v);
    } // < vAdd

    public static float[] vAddT(float[] v, float t) {
        // > adds a value to each sample of top wave

        return vAddTB(v, t, 0.0f);        
    } // < vAddT

    public static float[] vAddB(float[] v, float b) {
        // > adds a value to each sample of bottom wave

        return vAddTB(v, 0.0f, b);        
    } // < vAddB

    // ----------- static vector return methods --------------------
    // vector = vector + vector values
    public static float[] vAddTB(float[] v, float[] t, float[] b) {
        // > adds different values to top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );
        b = Signal.ExpandArray( b, e );        

        for (int i=0; i<e; i++){
            y[i] = v[i] + Signal.FetchT_B(v[i], t[i], b[i] );
        }        
        return y;
    } // < vAddTB

    public static float[] vAdd(float[] a, float[] v) {
        // > adds a value to each sample

        int e = v.length;
        float[] y = new float[ e ];

        v = Signal.ExpandArray( v, e );

        for (int i=0; i<e; i++){
            y[i] = a[i] + v[i];            
        }        
        return y;
    } // < vAdd

    public static float[] vAddT(float[] v, float[] t) {
        // > adds a value to each sample of top wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );

        for (int i=0; i<e; i++){
            if ( Signal.isPositive(v[i]) ){
                y[i] = v[i] + t[i];                
            }
        }        
        return y;
    } // < vAddT

    public static float[] vAddB(float[] v, float[] b) {
        // > adds a value to each sample of bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        b = Signal.ExpandArray( b, e );

        for (int i=0; i<e; i++){
            if ( !Signal.isPositive(v[i]) ){
                y[i] = v[i] + b[i];
            }
        }        
        return y;
    } // < vAddB

    // ---------------------------------------------------
    // signal + signal 
    public void AddTB(Signal t, Signal b) {
        // > adds different values to top and bottom wave

        if ( !Sel.Equals(t.getSel()) | (!Sel.Equals(b.getSel()) ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            float v = getItIndex(i); // current value
            setItIndex(i, v + Signal.FetchT_B( v, t.getItIndex(i), b.getItIndex(i) ) );            
        }
    } // < AddTB

    public void Add(Signal s) {
        // > adds a value to each sample

        if ( !Sel.Equals( s.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setItIndex(i, getItIndex(i) + s.getItIndex(i) );            
        }
    } // < Add

    public void AddT(Signal t) {
        // > adds a value to each sample of top wave     

        if ( !Sel.Equals( t.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) + t.getItIndex(i) );                            
            }
        }
    } // < AddT

    public void AddB(Signal b) {
        // > adds a value to each sample of bottom wave

        if ( !Sel.Equals( b.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( !isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) + b.getItIndex(i) );                            
            }
        }
    } // < AddB

    // ---------- MULTIPLICATION METHODS -------------
    // signal * values methods
    public void MultTB(float t, float b) {
        // > multiplies  different values on top and bottom wave

        toSelStart(); 
        while (inSel()){
            setItForward( getIt() * FetchT_B(t, b) );             
        }
    } // < MultTB

    public void Mult(float v) {
        // > multiplies  a value on each sample

        MultTB(v, v);
    } // < Mult

    public void MultT(float t) {
        // > multiplies  a value on each sample of top wave

        MultTB(t, 1.0f);        
    } // < MultT

    public void MultB(float b) {
        // > multiplies  a value on each sample of bottom wave

        MultTB(1.0f, b);        
    } // < MultB

    // ---------- vector arithmetic ------------------
    // siganl * vectors values
    public void MultTB(float[] t, float[] b) {
        // > multiplies  different values on vectors on top and bottom wave
        t = ExpandArray(t);
        b = ExpandArray(b);        

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setIt( getItIndex(i) * FetchT_B(t[i], b[i]) );            
        }
    } // < MultTB

    public void Mult(float[] v) {
        // > multiplies  vectors 
        int i=0;
        v = ExpandArray(v);        
        toSelStart(); 
        while (inSel()){
            setItForward( getIt() * v[i++] );
        }
    } // < Mult

    public void MultT( float[] t ) {
        // > multiplies  a value on each sample of top wave
        float[] b = null;
        MultTB( t, b );        
    } // < MultT

    public void MultB( float[] b ) {
        // > multiplies  a value on each sample of bottom wave
        float[] t = null;
        MultTB( t, b );        
    } // < MultB

    // ---------- vector return ------------------
    // vector = siganl * values
    public float[] vMultTB(float t, float b) {
        // > multiplies  different values on top and bottom wave
        int e = Sel.getLength();        
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) * FetchT_B( t, b );
        }
        return y;        
    } // < vMultTB

    public float[] vMult(float v) {
        // > multiplies  a value on each sample
        return vMultTB(v, v);
    } // < vMult

    public float[] vMultT(float t) {
        // > multiplies  a value on each sample of top wave
        return vMultTB(t, 1.0f);        
    } // < vMultT

    public float[] vMultB(float b) {
        // > multiplies  a value on each sample of bottom wave
        return vMultTB(1.0f, b);        
    } // < vMultB

    // -------------- vector in & out ---------------
    // vector = siganl * vector values
    public float[] vMultTB(float[] t, float[] b) {
        // > multiplies  different values on top and bottom wave
        // > and returns vector

        int e = Sel.getLength();
        float[] y = new float[ e ];

        t = ExpandArray( t );
        b = ExpandArray( b );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) * FetchT_B( t[i], b[i] );
        }
        return y;
    } // < vMultTB

    public float[] vMult(float[] v) {
        // > multiplies  vectors 

        int e = Sel.getLength();
        float[] y = new float[ e ];

        v = ExpandArray( v );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) * v[i];
        }
        return y;        
    } // < vMult

    public float[] vMultT(float[] t) {
        // > multiplies  a value on each sample of top wave
        // > and returns vector
        float[] b = null; 

        return vMultTB(t, b);        
    } // < vMultT

    public float[] vMultB(float[] b) {
        // > multiplies  a value on each sample of bottom wave
        // > and returns vector        
        float[] t = null; 

        return vMultTB(t, b);        
    } // < vMultB

    // ----------- static vector return methods --------------------
    // vector = vector * values
    public static float[] vMultTB(float[] v, float t, float b) {
        // > multiplies  different values on top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = v[i] * Signal.FetchT_B(v[i], t, b );
        }
        return y;
    } // < vMultTB

    public static float[] vMult(float[] a, float v) {
        // > multiplies  a value on each sample

        return vMultTB(a, v, v);
    } // < vMult

    public static float[] vMultT(float[] v, float t) {
        // > multiplies  a value on each sample of top wave

        return vMultTB(v, t, 1.0f);        
    } // < vMultT

    public static float[] vMultB(float[] v, float b) {
        // > multiplies  a value on each sample of bottom wave

        return vMultTB(v, 1.0f, b);        
    } // < vMultB

    // ----------- static vector return methods --------------------
    // vector = vector * vector values
    public static float[] vMultTB(float[] v, float[] t, float[] b) {
        // > multiplies  different values on top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );
        b = Signal.ExpandArray( b, e );        

        for (int i=0; i<e; i++){
            y[i] = v[i] * Signal.FetchT_B(v[i], t[i], b[i] );
        }        
        return y;
    } // < vMultTB

    public static float[] vMult(float[] a, float[] v) {
        // > multiplies  a value on each sample

        int e = v.length;
        float[] y = new float[ e ];

        v = Signal.ExpandArray( v, e );

        for (int i=0; i<e; i++){
            y[i] = a[i] * v[i];            
        }        
        return y;
    } // < vMult 

    public static float[] vMultT(float[] v, float[] t) {
        // > multiplies  a value on each sample of top wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );

        for (int i=0; i<e; i++){
            if ( Signal.isPositive(v[i]) ){
                y[i] = v[i] * t[i];                
            }
        }        
        return y;
    } // < vMultT

    public static float[] vMultB(float[] v, float[] b) {
        // > multiplies  a value on each sample of bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        b = Signal.ExpandArray( b, e );

        for (int i=0; i<e; i++){
            if ( !Signal.isPositive(v[i]) ){
                y[i] = v[i] * b[i];
            }
        }        
        return y;
    } // < vMultB

    // ---------------------------------------------------
    // signal * signal 
    public void MultTB(Signal t, Signal b) {
        // > multiplies  different values on top and bottom wave

        if ( !Sel.Equals(t.getSel()) | (!Sel.Equals(b.getSel()) ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            float v = getItIndex(i); // current value
            setItIndex(i, v * Signal.FetchT_B( v, t.getItIndex(i), b.getItIndex(i) ) );            
        }
    } // < MultTB

    public void Mult(Signal s) {
        // > multiplies  a value on each sample

        if ( !Sel.Equals( s.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setItIndex(i, getItIndex(i) * s.getItIndex(i) );            
        }
    } // < Mult

    public void MultT(Signal t) {
        // > multiplies  a value on each sample of top wave     

        if ( !Sel.Equals( t.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) * t.getItIndex(i) );                            
            }
        }
    } // < MultT

    public void MultB(Signal b) {
        // > multiplies  a value on each sample of bottom wave

        if ( !Sel.Equals( b.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( !isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) * b.getItIndex(i) );                            
            }
        }
    } // < MultB

    // -------------- DIVIDE  METHODS ----------------
    // signal / values methods
    public void DivTB(float t, float b) {
        // > divides  different values on top and bottom wave

        if ( t == 0.0f ) { t = getVirtualZero(); }
        if ( b == 0.0f ) { b = getVirtualZero(); }        

        toSelStart(); 
        while (inSel()){
            setItForward( getIt() / FetchT_B(t, b) );             
        }
    } // < DivTB

    public void Div(float v) {
        // > divides  a value on each sample

        DivTB(v, v);
    } // < Div

    public void DivT(float t) {
        // > divides  a value on each sample of top wave

        DivTB(t, 1.0f);        
    } // < DivT

    public void DivB(float b) {
        // > divides  a value on each sample of bottom wave

        DivTB(1.0f, b);        
    } // < DivB

    // ---------- vector return ------------------
    // vector = siganl / values
    public float[] vDivTB(float t, float b) {
        // > divides  different values on top and bottom wave

        if ( t == 0.0f ) { t = getVirtualZero(); }
        if ( b == 0.0f ) { b = getVirtualZero(); }        

        int e = Sel.getLength();        
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) / FetchT_B( t, b );
        }
        return y;        
    } // < vDivTB

    public float[] vDiv(float v) {
        // > divides  a value on each sample
        return vDivTB(v, v);
    } // < vDiv

    public float[] vDivT(float t) {
        // > divides  a value on each sample of top wave
        return vDivTB(t, 1.0f);        
    } // < vDivT

    public float[] vDivB(float b) {
        // > divides  a value on each sample of bottom wave
        return vDivTB(1.0f, b);        
    } // < vDivB

    // ----------- static vector return methods --------------------
    // vector = vector / values
    public static float[] vDivTB(float[] v, float t, float b) {
        // > divides  different values on top and bottom wave

        if ( t == 0.0f ) { t = 0.01f; }
        if ( b == 0.0f ) { b = 0.01f; }        

        int e = v.length;
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = v[i] / Signal.FetchT_B(v[i], t, b );
        }
        return y;
    } // < vDivTB

    public static float[] vDiv(float[] a, float v) {
        // > divides  a value on each sample

        return vDivTB(a, v, v);
    } // < vDiv

    public static float[] vDivT(float[] v, float t) {
        // > divides  a value on each sample of top wave

        return vDivTB(v, t, 1.0f);        
    } // < vDivT

    public static float[] vDivB(float[] v, float b) {
        // > divides  a value on each sample of bottom wave

        return vDivTB(v, 1.0f, b);        
    } // < vDivB

    // -------------- SUB   METHODS ----------------
    // signal - values methods
    public void SubTB(float t, float b) {
        // > substracts  different values from top and bottom wave

        toSelStart(); 
        while (inSel()){
            setItForward( getIt() - FetchT_B(t, b) );             
        }
    } // < SubTB

    public void Sub(float v) {
        // > substracts  a value from each sample

        SubTB(v, v);
    } // < Sub

    public void SubT(float t) {
        // > substracts  a value from each sample of top wave

        SubTB(t, 0.0f);        
    } // < SubT

    public void SubB(float b) {
        // > substracts  a value from each sample of bottom wave

        SubTB(0.0f, b);        
    } // < SubB

    // ---------- vector arithmetic ------------------
    // siganl - vectors values
    public void SubTB(float[] t, float[] b) {
        // > substracts  different values from vectors from top and bottom wave
        t = ExpandArray(t);
        b = ExpandArray(b);        

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setIt( getItIndex(i) - FetchT_B(t[i], b[i]) );            
        }
    } // < SubTB

    public void Sub(float[] v) {
        // > substracts  vectors 
        int i=0;
        v = ExpandArray(v);        
        toSelStart(); 
        while (inSel()){
            setItForward( getIt() - v[i++] );
        }
    } // < Sub

    public void SubT( float[] t ) {
        // > substracts  a value from each sample of top wave
        float[] b = null;
        SubTB( t, b );        
    } // < SubT

    public void SubB( float[] b ) {
        // > substracts  a value from each sample of bottom wave
        float[] t = null;
        SubTB( t, b );        
    } // < SubB

    // ---------- vector return ------------------
    // vector = siganl - values
    public float[] vSubTB(float t, float b) {
        // > substracts  different values from top and bottom wave
        int e = Sel.getLength();        
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) - FetchT_B( t, b );
        }
        return y;        
    } // < vSubTB

    public float[] vSub(float v) {
        // > substracts  a value from each sample
        return vSubTB(v, v);
    } // < vSub

    public float[] vSubT(float t) {
        // > substracts  a value from each sample of top wave
        return vSubTB(t, 0.0f);        
    } // < vSubT

    public float[] vSubB(float b) {
        // > substracts  a value from each sample of bottom wave
        return vSubTB(0.0f, b);        
    } // < vSubB

    // -------------- vector in & out ---------------
    // vector = siganl - vector values
    public float[] vSubTB(float[] t, float[] b) {
        // > substracts  different values from top and bottom wave
        // > and returns vector

        int e = Sel.getLength();
        float[] y = new float[ e ];

        t = ExpandArray( t );
        b = ExpandArray( b );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) - FetchT_B( t[i], b[i] );
        }
        return y;
    } // < vSubTB

    public float[] vSub(float[] v) {
        // > substracts  vectors 

        int e = Sel.getLength();
        float[] y = new float[ e ];

        v = ExpandArray( v );        

        for (int i=0; i<e; i++){
            y[i] = getItIndex( i ) - v[i];
        }
        return y;        
    } // < vSub

    public float[] vSubT(float[] t) {
        // > substracts  a value from each sample of top wave
        // > and returns vector
        float[] b = null; 

        return vSubTB(t, b);        
    } // < vSubT

    public float[] vSubB(float[] b) {
        // > substracts  a value from each sample of bottom wave
        // > and returns vector        
        float[] t = null; 

        return vSubTB(t, b);        
    } // < vSubB

    // ----------- static vector return methods --------------------
    // vector = vector - values
    public static float[] vSubTB(float[] v, float t, float b) {
        // > substracts  different values from top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        for (int i=0; i<e; i++){
            y[i] = v[i] - Signal.FetchT_B(v[i], t, b );
        }
        return y;
    } // < vSubTB

    public static float[] vSub(float[] a, float v) {
        // > substracts  a value from each sample

        return vSubTB(a, v, v);
    } // < vSub

    public static float[] vSubT(float[] v, float t) {
        // > substracts  a value from each sample of top wave

        return vSubTB(v, t, 0.0f);        
    } // < vSubT

    public static float[] vSubB(float[] v, float b) {
        // > substracts  a value from each sample of bottom wave

        return vSubTB(v, 0.0f, b);        
    } // < vSubB

    // ----------- static vector return methods --------------------
    // vector = vector - vector values
    public static float[] vSubTB(float[] v, float[] t, float[] b) {
        // > substracts  different values from top and bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );
        b = Signal.ExpandArray( b, e );        

        for (int i=0; i<e; i++){
            y[i] = v[i] - Signal.FetchT_B(v[i], t[i], b[i] );
        }        
        return y;
    } // < vSubTB

    public static float[] vSub(float[] a, float[] v) {
        // > substracts  a value from each sample

        int e = v.length;
        float[] y = new float[ e ];

        v = Signal.ExpandArray( v, e );

        for (int i=0; i<e; i++){
            y[i] = a[i] - v[i];            
        }        
        return y;
    } // < vSub 

    public static float[] vSubT(float[] v, float[] t) {
        // > substracts  a value from each sample of top wave

        int e = v.length;
        float[] y = new float[ e ];

        t = Signal.ExpandArray( t, e );

        for (int i=0; i<e; i++){
            if ( Signal.isPositive(v[i]) ){
                y[i] = v[i] - t[i];                
            }
        }        
        return y;
    } // < vSubT

    public static float[] vSubB(float[] v, float[] b) {
        // > substracts  a value from each sample of bottom wave

        int e = v.length;
        float[] y = new float[ e ];

        b = Signal.ExpandArray( b, e );

        for (int i=0; i<e; i++){
            if ( !Signal.isPositive(v[i]) ){
                y[i] = v[i] - b[i];
            }
        }        
        return y;
    } // < vSubB

    // ---------------------------------------------------
    // signal - signal 
    public void SubTB(Signal t, Signal b) {
        // > substracts  different values from top and bottom wave

        if ( !Sel.Equals(t.getSel()) | (!Sel.Equals(b.getSel()) ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            float v = getItIndex(i); // current value
            setItIndex(i, v - Signal.FetchT_B( v, t.getItIndex(i), b.getItIndex(i) ) );            
        }
    } // < SubTB

    public void Sub(Signal s) {
        // > substracts  a value from each sample

        if ( !Sel.Equals( s.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            setItIndex(i, getItIndex(i) - s.getItIndex(i) );            
        }
    } // < Sub

    public void SubT(Signal t) {
        // > substracts  a value from each sample of top wave     

        if ( !Sel.Equals( t.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) - t.getItIndex(i) );                            
            }
        }
    } // < SubT

    public void SubB(Signal b) {
        // > substracts  a value from each sample of bottom wave

        if ( !Sel.Equals( b.getSel() ) ) { return; }
        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( !isPositiveIndex(i) ){
                setItIndex(i, getItIndex(i) - b.getItIndex(i) );                            
            }
        }
    } // < SubB

    // -------------- POWER  METHODS ----------------

    public void PowerTB(float t, float b) {
        // > raises top & bottom _s[i] to power t & b respectively

        toSelStart(); 
        while (inSel()){
            if ( isPositive() ) {
                setItForward((float)Math.pow((double)getIt(),(double)t));
            } else {
                setItForward((float)Math.pow((double)getIt(),(double)b));                
            }
        }
    } // < PowerTB

    public void Power(float Value) {
        // > raises _s[i] to power Value 

        PowerTB(Value, Value);
    } // < Power

    public void PowerT(float t) {
        // > raises top _s[i] to power Value 

        PowerTB(t, 1.0f);       
    } // < PowerT

    public void PowerB(float b) {
        // > raises bottom _s[i] to power Value 

        PowerTB(1.0f, b);
    } // < PowerB


    // -------------- MISC  METHODS ----------------

    public void Positive() {
        // > gives absolute value to each sample
        // the same as invert all negatives

        toSelStart(); 
        while (inSel()){
            setItForward( Math.abs( getIt() ) );
        }
    } // < Positive

    public static float[] vPositive(float[] a) {
        // > gives Negative value to each sample in a[]
        for (int i=0; i<a.length; i++){
            a[i] = Math.abs( a[i] );
        }
        return a;
    } // < vPositive

    public void Negative() {
        // > gives Negative value to each sample
        // the same as invert all positives        

        toSelStart(); 
        while (inSel()){
            setItForward( -Math.abs( getIt() ) );
        }
    } // < Negative

    public static float[] vNegative(float[] a) {
        // > gives Negative value to each sample in a[]
        for (int i=0; i<a.length; i++){
            a[i] = -Math.abs( a[i] );
        }
        return a;
    } // < vNegative

    public void Invert() {
        // > inverts all samples within selection

        toSelStart(); 
        while (inSel()) {
            setItForward(-getIt());             
        };
    } // < Invert

    public static float[] vInvert(float[] a) {
        // > gives Negative value to each sample in a[]
        for (int i=0; i<a.length; i++){
            a[i] = -a[i];
        }
        return a;
    } // < vInvert

    // -----------------------------------------------
    // ---------------- max ------------------
    public float MaxAbsolute() {
        // > finds absolute max value within selection
        float maxValue=0;

        toSelStart(); 
        while (inSel()) {
            maxValue = Math.max( maxValue, Math.abs(getIt()) );
            nextPos();
        };
        return maxValue;
    } // < MaxAbsolute

    public float Max() {
        // > finds positive max value within selection
        float maxValue=0;

        toSelStart(); 
        while (inSel()) {
            maxValue = Math.max( maxValue, getItForward() );
        };
        return maxValue;
    } // < Max

    public static float Max(float[] a) {
        // > finds negative max value in array a
        float maxValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            maxValue = Math.max( maxValue, a[i] );            
        }
        return maxValue;
    } // < Max

    public float MaxT() {
        // > finds negative max value in top 
        float maxValue=0;

        toSelStart(); 
        while (inSel()) {
            if ( isPositive() ){
                maxValue = Math.max( maxValue, getItForward() );
            }
        };
        return maxValue;
    } // < MaxT

    public static float MaxT(float[] a) {
        // > finds negative max value in top in array a 
        float maxValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            if ( Signal.isPositive(a[i]) ){            
                maxValue = Math.max( maxValue, a[i] );            
            }
        }
        return maxValue;
    } // < MaxT

    public float MaxB() {
        // > finds negative max value in top 
        float maxValue=0;

        toSelStart(); 
        while (inSel()) {
            if ( !isPositive() ){
                maxValue = Math.max( maxValue, getItForward() );
            }
        };
        return maxValue;
    } // < MaxB

    public static float MaxB(float[] a) {
        // > finds negative max value in top in array a 
        float maxValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            if ( !Signal.isPositive(a[i]) ){            
                maxValue = Math.max( maxValue, a[i] );            
            }
        }
        return maxValue;
    } // < MaxB

    // ---------------- min ------------------
    public float Min() {
        // > finds negative min value within selection
        float minValue=0;

        toSelStart(); 
        while (inSel()) {
            minValue = Math.min( minValue, getItForward() );
        };
        return minValue;
    } // < Min

    public static float Min(float[] a) {
        // > finds negative min value in array a
        float minValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            minValue = Math.min( minValue, a[i] );            
        }
        return minValue;
    } // < Min

    public float MinT() {
        // > finds negative min value in top 
        float minValue=0;

        toSelStart(); 
        while (inSel()) {
            if ( isPositive() ){
                minValue = Math.min( minValue, getItForward() );
            }
        };
        return minValue;
    } // < MinT

    public static float MinT(float[] a) {
        // > finds negative min value in top in array a 
        float minValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            if ( Signal.isPositive(a[i]) ){            
                minValue = Math.min( minValue, a[i] );            
            }
        }
        return minValue;
    } // < MinT

    public float MinB() {
        // > finds negative min value in top 
        float minValue=0;

        toSelStart(); 
        while (inSel()) {
            if ( !isPositive() ){
                minValue = Math.min( minValue, getItForward() );
            }
        };
        return minValue;
    } // < MinB

    public static float MinB(float[] a) {
        // > finds negative min value in top in array a 
        float minValue=0;

        int e = a.length;
        for (int i=0; i<e; i++){
            if ( !Signal.isPositive(a[i]) ){            
                minValue = Math.min( minValue, a[i] );            
            }
        }
        return minValue;
    } // < MinB

    // ---------------- min & max ------------------
    public float[] MinAndMax() {
        // > returns min & max values in float[2] array
        // 
        float minValue=0;
        float maxValue=0;        
        float[] tmp;                

        toSelStart(); 
        while (inSel()) {
            minValue = Math.min( minValue, getIt() );
            maxValue = Math.max( maxValue, getIt() );            
            nextPos();
        };

        tmp = new float[2];
        tmp[0] = minValue;          tmp[1] = maxValue;
        return tmp;
    } // < MinAndMax


    // ------------------------------------------------
    public float Mean() {
        // > finds mean value within selection
        // 
        double meanValue=0;
        double sum=0;

        toSelStart(); 
        while (inSel()) {
            sum += getItForward();
        };
        meanValue = sum/Sel.getLength();        
        return (float)meanValue;
    } // < Mean

    public float MeanT() {
        // > finds mean value within selection
        // 
        double meanValue=0;
        double sum=0;

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositive() ){
                sum += getItIndex(i);
            }
        }
        meanValue = sum/Sel.getLength();        
        return (float)meanValue;
    } // < MeanT

    public float MeanB() {
        // > finds mean value within selection
        // 
        double meanValue=0;
        double sum=0;

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( !isPositive() ){
                sum += getItIndex(i);
            }
        }
        meanValue = sum/Sel.getLength();        
        return (float)meanValue;
    } // < MeanB

    // ------------------------------------------------
    public float[] getRoundError() {
        // > returns error vector
        float[] y;
        int i = 0;

        y = new float[ Sel.getLength() ];    
        toSelStart(); 
        while (inSel()) {
            y[i] = getIt() - (float)getItAsShort();
            nextPos();
            i++;
        };
        return y;
    } // < getRoundError

    // ----------- peak normalization ----------------    
    public void NormTB(float Level) {
        // > scales signal within selection
        float maxT, maxB, factorT, factorB;

        maxT = MaxT();
        maxB = MaxB();        
        factorT = Level/maxT;
        factorB = Level/maxB;        
        MultTB( factorT, factorB );
    } // < NormTB

    public void Norm(float Level) {
        // > scales signal within selection
        float maxValue, maxFactor;

        maxValue = MaxAbsolute();
        maxFactor = Level/maxValue;
        Mult(maxFactor);
    } // < Norm

    public void NormT(float Level) {
        // > scales signal within selection
        float   factorT = Level/MaxT();
        MultTB( factorT, 1.0f );
    } // < NormT

    public void NormB(float Level) {
        // > scales signal within selection
        float   factorB = Level/MaxB();
        MultTB( 1.0f, factorB );
    } // < NormB

    // ------------------------------------------------
    public void RemoveDC() {
        // > finds offset and remove it within selection
        // 
        float offset; 

        offset = Mean();
        Add(-offset);
    } // < RemoveDC

    // ***************************************************
    // * LOGICAL methods
    // * date: 04/07/99
    // ***************************************************
    // --------------------------------------------------

    public void LogicAdd1(float v) {
        // > if (signs are equal) { keep; } else { add; }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) != Signal.isPositive(v) ) {
                setItIndex( i, getItIndex(i)+v );                
            }            
        }
    } // < LogicAdd1


    public void LogicAdd1(float[] v) {
        // > if (signs are equal) { keep; } else { add; }
        v = ExpandArray(v);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) != Signal.isPositive(v[i]) ) {
                setItIndex( i, getItIndex(i)+v[i] );                
            }            
        }
    } // < LogicAdd1

    public void LogicAdd2(float v) {
        // > if (signs are equal) { add; } else { keep;  }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) == Signal.isPositive(v) ) {
                setItIndex( i, getItIndex(i)+v);                
            }            
        }
    } // < LogicAdd2

    public void LogicAdd2(float[] v) {
        // > if (signs are equal) { add; } else { keep;}
        v = ExpandArray(v);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( isPositiveIndex(i) == Signal.isPositive(v[i]) ) {
                setItIndex( i, getItIndex(i)+v[i]);                
            }            
        }
    } // < LogicAdd2

    // ------------------- thresholding/distortion ------------------------
    // -------- value limiting ---------------------    
    public void LimitTB(float t, float b) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            float v = FetchIndexT_B( i, t, b );
            if ( Math.abs( getItIndex(i) ) > Math.abs(v) ) {
                setItIndex( i, v );                
            }            
        }
    } // < LimitTB

    public void Limit(float v) {
        // > if _s[] > v { _s[]=v } else { _s[]=_s[] }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( Math.abs(getItIndex(i)) > Math.abs(v) ) {
                if ( isPositiveIndex(i) ){
                    setItIndex( i, v );
                } else {
                    setItIndex( i, -v );      
                }
            }            
        }
    } // < Limit

    public void LimitT(float t) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( ( Math.abs(getItIndex(i)) > Math.abs(t) ) & ( isPositiveIndex(i) ) ) {
                setItIndex( i, t);                
            }            
        }
    } // < LimitT

    public void LimitB(float b) {
        // > if _s[] > b { _s[]=b } else { _s[]=_s[] }

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( ( Math.abs(getItIndex(i)) > Math.abs(b) ) & ( !isPositiveIndex(i) ) ) {
                setItIndex( i, b);   
            }            
        }
    } // < LimitB

    // ------- vector limiting --------------
    public void LimitTB(float[] t, float[] b) {
        // > if _s[] > t[i] { _s[]=t[i] } else { _s[]=_s[] }

        t = ExpandArray(t);
        b = ExpandArray(b);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){            
            float v = FetchIndexT_B( i, t[i], b[i] );            
            if ( Math.abs(getItIndex(i)) > Math.abs(v) ) {
                setItIndex( i, v );                
            }            
        }
    } // < LimitTB

    public void Limit(float[] v) {
        // > if _s[] > v[] { _s[]=v[] } else { _s[]=_s[] }

        v = ExpandArray(v);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( Math.abs(getItIndex(i)) > Math.abs(v[i]) ) {
                if ( isPositiveIndex(i) ){
                    setItIndex( i, v[i] );
                } else {
                    setItIndex( i, -v[i] );      
                }
            }            
        }
    } // < Limit

    public void LimitT(float[] t) {
        // > if _s[] > t[] { _s[]=t[] } else { _s[]=_s[] }

        t = ExpandArray(t);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( ( Math.abs(getItIndex(i)) > Math.abs(t[i]) ) & ( isPositiveIndex(i) ) ) {
                setItIndex( i, t[i] );                
            }            
        }
    } // < LimitT

    public void LimitB(float[] b) {
        // > if _s[] > b[] { _s[]=b[] } else { _s[]=_s[] }

        b = ExpandArray(b);

        int e = Sel.getLength();
        for (int i=0; i<e; i++){
            if ( ( Math.abs(getItIndex(i)) > Math.abs(b[i]) ) & ( !isPositiveIndex(i) ) ) {
                setItIndex( i, b[i] );   
            }            
        }
    } // < LimitB

    // -------- array return value limiting ---------------------    
    public float[] vLimitTB(float t, float b) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float v = FetchIndexT_B( i, t, b );
            float g = getItIndex(i);
            if ( Math.abs( g ) > Math.abs(v) ) {
                y[i] = v;
            } else {
                y[i] = g ;
            }
        }
        return y;
    } // < vLimitTB

    public float[] vLimit(float v) {
        // > if _s[] > v { _s[]=v } else { _s[]=_s[] }

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( Math.abs( g ) > Math.abs(v) ) {
                if ( isPositiveIndex(i) ){
                    y[i] = v;                    
                } else {
                    y[i] = -v;
                }                
            } else {
                y[i] = g ;
            }

        }
        return y;        
    } // < vLimit

    public float[] vLimitT(float t) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( ( Math.abs( g ) > Math.abs(t) ) & ( isPositiveIndex(i) ) ) {
                y[i] = t;
            } else {
                y[i] = g ;
            }

        }
        return y;        
    } // < vLimitT

    public float[] vLimitB(float b) {
        // > if _s[] > b { _s[]=b } else { _s[]=_s[] }

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( ( Math.abs( g ) > Math.abs(b) ) & ( !isPositiveIndex(i) ) ) {
                y[i] = b;                
            } else {
                y[i] = g ;
            }

        }
        return y;        
    } // < vLimitB

    // -------- array return array inpit limiting ---------------------        
    public float[] vLimitTB(float[] t, float[] b) {
        // > if _s[] > t[] { _s[]=t[] } else { _s[]=_s[] }

        t = ExpandArray(t);
        b = ExpandArray(b);

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float v = FetchIndexT_B( i, t[i], b[i] );
            float g = getItIndex(i);
            if ( Math.abs( g ) > Math.abs(v) ) {
                y[i] = v;
            } else {
                y[i] = g ;
            }

        }
        return y;
    } // < vLimitTB

    public float[] vLimit(float[] v) {
        // > if _s[] > v { _s[]=v } else { _s[]=_s[] }

        v = ExpandArray(v);

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( Math.abs( g ) > Math.abs(v[i]) ) {
                if ( isPositiveIndex(i) ){
                    y[i] = v[i];                    
                } else {
                    y[i] = -v[i];
                }                
            } else {
                y[i] = g ;
            }

        }
        return y;        
    } // < vLimit

    public float[] vLimitT(float[] t) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        t = ExpandArray(t);

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( ( Math.abs( g ) > Math.abs(t[i]) ) & ( isPositiveIndex(i) ) ) {
                y[i] = t[i];
            } else {
                y[i] = g ;
            }

        }
        return y;        
    } // < vLimitT

    public float[] vLimitB(float[] b) {
        // > if _s[] > b { _s[]=b } else { _s[]=_s[] }

        b = ExpandArray(b);

        int e = Sel.getLength();
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float g = getItIndex(i);            
            if ( ( Math.abs( g ) > Math.abs(b[i]) ) & ( !isPositiveIndex(i) ) ) {
                y[i] = b[i];                
            } else {
                y[i] = g ;
            }
        }
        return y;        
    } // < vLimitB

    // -------- static array return value limiting ---------------------    
    public static float[] vLimitTB(float[] a, float t, float b) {
        // > if a[] > t { a[]=t } else { a[]=a[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float v = Signal.FetchT_B( a[i], t, b );
            if ( Math.abs( a[i] ) > Math.abs(v) ) {
                y[i] = v;
            } else {
                y[i] = a[i] ;
            }
        }
        return y;
    } // < vLimitTB

    public static float[] vLimit(float[] a, float v) {
        // > if _s[] > v { _s[]=v } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){

            if ( Math.abs( a[i] ) > Math.abs(v) ) {
                if ( isPositive(a[i]) ){
                    y[i] = v;                    
                } else {
                    y[i] = -v;
                }                
            } else {
                y[i] = a[i] ;
            }

        }
        return y;        
    } // < vLimit

    public static float[] vLimitT(float[] a, float t) {
        // > if _s[] > t { _s[]=t } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){          
            if ( ( Math.abs( a[i] ) > Math.abs(t) ) & ( isPositive(a[i]) ) ) {
                y[i] = t;
            } else {
                y[i] = a[i] ;
            }
        }
        return y;        
    } // < vLimitT

    public static float[] vLimitB(float[] a, float b) {
        // > if _s[] > b { _s[]=b } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){          
            if ( ( Math.abs( a[i] ) > Math.abs(b) ) & ( !isPositive(a[i]) ) ) {
                y[i] = b;                
            } else {
                y[i] = a[i] ;
            }
        }
        return y;        
    } // < vLimitB

    // -------- static array return array limiting ---------------------    
    public static float[] vLimitTB(float[] a, float[] t, float[] b) {
        // > if a[] > t { a[]=t } else { a[]=a[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){
            float v = Signal.FetchT_B( a[i], t[i], b[i] );
            if ( Math.abs( a[i] ) > Math.abs(v) ) {
                y[i] = v;
            } else {
                y[i] = a[i] ;
            }
        }
        return y;
    } // < vLimitTB

    public static float[] vLimit(float[] a, float[] v) {
        // > if _s[] > v[i] { _s[]=v[i] } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){

            if ( Math.abs( a[i] ) > Math.abs(v[i]) ) {
                if ( isPositive(a[i]) ){
                    y[i] = v[i];                    
                } else {
                    y[i] = -v[i];
                }                
            } else {
                y[i] = a[i] ;
            }

        }
        return y;        
    } // < vLimit

    public static float[] vLimitT(float[] a, float[] t) {
        // > if _s[] > t[i] { _s[]=t[i] } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){          
            if ( ( Math.abs( a[i] ) > Math.abs(t[i]) ) & ( isPositive(a[i]) ) ) {
                y[i] = t[i];
            } else {
                y[i] = a[i] ;
            }
        }
        return y;        
    } // < vLimitT

    public static float[] vLimitB(float[] a, float[] b) {
        // > if _s[] > b[i] { _s[]=b[i] } else { _s[]=_s[] }

        int e = a.length;
        float[] y = new float[e];

        for (int i=0; i<e; i++){          
            if ( ( Math.abs( a[i] ) > Math.abs(b[i]) ) & ( !isPositive(a[i]) ) ) {
                y[i] = b[i];                
            } else {
                y[i] = a[i] ;
            }
        }
        return y;        
    } // < vLimitB

    // ----------------- noise gating ---------------------
    public void NoiseGate(float Threshold) {
        // > if _s[] < Threshold { _s[]=0.0f} else { _s[]=_s[] }
        // it's the same as Noise-Gating

        toSelStart(); 
        while (inSel()){
            if ( Math.abs(getIt()) < Threshold ) {
                setItForward(0.0f);                                
            } else {
                nextPos();
            }
        }
    } // < NoiseGate

    // ------------ distortion -----------------------------------
    public void Limit(float Threshold, float ClampLevel) {
        // > if _s[] > Threshold { _s[]=Threshold } else { _s[]=_s[] }
        // Clamping fx is the same

        toSelStart(); 
        while (inSel()){
            if ( Math.abs(getIt()) > Threshold ) {
                // FIXME!!! for negatives!
                setItForward(ClampLevel);                                
            } else {
                nextPos();
            }
        }
    } // < Limit

    public void NoiseGate2(float Threshold) {
        // > if sign(_s[]-Threshold) != sign(_s[]) { _s[]=0.0f } else { _s[]=_s[]-Threshold; }

        float tmp;

        toSelStart(); 
        while (inSel()){
            tmp = _s[_Pos]-Threshold;
            if ((_s[_Pos]>=0) & (tmp>=0)){ // FIXME!!! >= or > ???
                setItForward(tmp);                
            } else {
                setItForward(0.0f);
            }
            if ((_s[_Pos]<0) & (tmp<0)){ // FIXME!!! 
                setItForward(tmp);                
            } else {
                setItForward(0.0f);
            }

        }
    } // < NoiseGate2

    // ***************************************************
    // * EDITING methods
    // * date: 05/07/99
    // ***************************************************

    public float[] getSelection(){
        // retuns selection part of the _s[]
        // acts as copy routine
        float[] tmp;

        tmp = new float[getSelectionLength()];
        System.arraycopy(_s, Sel.getStart(),tmp,0,getSelectionLength());
        return tmp;
    } //getSelection

    public void TrimSelection(){
        // retuns selection part of the _s[]
        float[] tmp;

        tmp = new float[getSelectionLength()];
        System.arraycopy(_s, Sel.getStart(),tmp,0,getSelectionLength());
        _s = tmp;
    } //TrimSelection

    public void DeleteSelection(){
        // deletes selection part of the _s[]
        float[] tmp;
        int     tail_length, front_length;

        toSelStart();
        front_length = getDistanceToStart();

        toSelEnd();
        tail_length = getDistanceToEnd();   

        tmp = new float[front_length + tail_length];

        System.arraycopy(_s, 0, tmp, 0, front_length );             
        System.arraycopy(_s, Sel.getEnd() + 1, tmp, front_length, tail_length );

        selectAll();        
        _s = tmp;

    } //DeleteSelection

    public void Append(float[] Vector){
        // adds Vector to the end of _s[]
        // selection remains untouched
        float[] tmp;

        tmp = new float[_s.length + Vector.length];

        System.arraycopy(_s, 0, tmp, 0, _s.length );             
        System.arraycopy(Vector, 0, tmp, _s.length, Vector.length );

        _s = tmp;

    } //Append (float[])

    public void Append(Signal sig2){
        // _s[] = _s[] + sig2._s[]
        // selection remains untouched
        float[] tmp;

        if (!RatesAreEqual(this, sig2)) { return; };

        tmp = new float[_s.length + sig2._s.length];

        System.arraycopy(_s, 0, tmp, 0, _s.length );             
        System.arraycopy(sig2._s, 0, tmp, _s.length, sig2._s.length );

        _s = tmp;
    } //Append (Signal)


    public void AppendVoid(int Length){
        // adds void vector to the end of _s[]
        // selection remains untouched
        float[] tmp, voidVector;

        voidVector = new float[Length];
        int i;
        for (i=0;i<voidVector.length-1;i++){
            voidVector[i]=0.0f;
        }
        tmp = new float[_s.length + Length];

        System.arraycopy(_s, 0, tmp, 0, _s.length );             
        System.arraycopy(voidVector, 0, tmp, _s.length, voidVector.length );

        _s = tmp;

    } //AppendVoid

    public void InsertAtStart(float[] Vector){
        // adds Vector to the start of _s[]
        // selection changes
        float[] tmp;

        tmp = new float[_s.length + Vector.length];

        System.arraycopy(Vector, 0, tmp, 0, Vector.length );             
        System.arraycopy(_s, 0, tmp, _s.length, _s.length );

        _s = tmp;

        // validate selection
        Sel.setStart(Sel.getStart() + Vector.length); 
        Sel.setEnd( Sel.getEnd() + Vector.length );
    } //InsertAtStart (float[])


    public float[] CutSelection(){
        // cuts selection part of the _s[]
        float[] tmp;

        tmp = getSelection();
        DeleteSelection();

        return tmp;
    } //CutSelection

    // ***************************************************
    // * GENERATING methods
    // * date: 05/07/99
    // ***************************************************
    public float getRandom(float Level, float center){    
        return ( center + Level * (float)( Math.random() - 0.5f ) );
    }

    public float[] gLinear(float a, float b, int start, int length){
        // generates linear function 
        float[] y;
        int end;

        end = start+length-1;        

        y = new float[length];

        for (int i=start;i<=end;i++){
            y[i]=a+b*i;
        }

        return y;
    } //gLinear

    public float[] gLinearOnPoints(float a, float b, int[] points){
        // generates linear function on given points
        // in general the result function is unlinear
        float[] y;        

        y = new float[points.length ];

        for (int i=0;i<=points.length-1;i++){
            y[i]=(float)(a+b*points[i]);
        }

        return y;
    } //gLinear

    public float[] gParabola(float a, float b, float c, int start, int length){
        // generates parabola function 
        float[] y;
        int end;

        end = start+length-1;

        y = new float[length];

        for (int i=start;i<=end;i++){
            y[i]=a+b*i+c*(float)(Math.pow(i,2));
        }

        return y;
    } //gParabola

    public float[] gRevert(float coef, int start, int length){
        // generates Revert function 
        float[] y;
        int end;

        end = start+length-1;

        y = new float[length];
        int i;

        for (i=start;i<=end;i++){
            if (i==0) { 
                y[i]=coef/0.01f; i++; 
            } else {            
                y[i]=coef/i;
            }
        }

        return y;
    } //gRevert

    public float[] gSquarePeriod(float Period, float Level){
        // generates 1 period of square function 
        // Period is in msec
        float[] y;
        int end;
        int PeriodInSamples, semiPeriod;

        PeriodInSamples = MsecToSamples(Period);
        semiPeriod = (int) (PeriodInSamples/2);  // FIXME!!!  if PeriodInSamples - even - ...

        y = new float[PeriodInSamples];       

        for (int i=0;i<semiPeriod;i++){
            y[i]=Level;
        }
        for (int i=semiPeriod;i<PeriodInSamples;i++){
            y[i]=-Level;
        }

        return y;
    } //gSquarePeriod

    public float[] gSawPeriod(float Period, float Level){
        // generates 1 period of square function 
        // Period is in msec
        float[] y;
        int end;
        int PeriodInSamples, semiPeriod;

        PeriodInSamples = MsecToSamples(Period);
        semiPeriod = (int) (PeriodInSamples/2);  // FIXME!!!  if PeriodInSamples - even - ...

        y = new float[PeriodInSamples];       
        y[0]=0.0f; // FIXME!!!!

        for (int i=1;i<semiPeriod;i++){
            y[i]=Level;
        }
        for (int i=semiPeriod;i<PeriodInSamples;i++){
            y[i]=-Level;
        }

        return y;
    } //gSawPeriod

    public void AddWhiteNoise(float Level){
        // generates noise

        toSelStart(); 
        while(inSel()) {
            setItForward(getIt()+getRandom(Level, 0));
        }
    } //gWhiteNoise


    // ***************************************************
    // * CONVERSION methods
    // * date: 07/07/99
    // ***************************************************

    public int MsecToSamplesOnRate(float Msec, int Rate){
        // converts msecs to samples on given samplerate
        return (int)(Msec*(float)Rate/1000);
    } // < MsecToSamplesOnRate

    public int MsecToSamples(float Msec){
        // converts msecs to samples on iternal _rate
        return MsecToSamplesOnRate(Msec, _rate); 
    } // < MsecToSamples

    public float SamplesToMsecOnRate(int Samples, int Rate){
        // converts samples to msecs on given samplerate
        return (float)(Samples*1000/Rate);
    } // < SamplesToMsecOnRate

    public float SamplesToMsec(int Samples){
        // converts samples to msecs on given samplerate
        return SamplesToMsecOnRate(Samples, _rate); 
    } // < SamplesToMsec
}

