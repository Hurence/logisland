package com.caseystella.analytics.outlier.batch;

import com.caseystella.analytics.outlier.batch.rpca.RPCA;
import com.caseystella.analytics.outlier.batch.rpca.RPCAOutlierAlgorithm;

public enum OutlierAlgorithms {
    RAD(RPCAOutlierAlgorithm.class);

    Class<? extends OutlierAlgorithm> clazz;
    OutlierAlgorithms(Class<? extends OutlierAlgorithm> clazz) {
        this.clazz = clazz;
    }
    public OutlierAlgorithm newInstance()  {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        }
    }
    public static OutlierAlgorithm newInstance(String outlierAlgorithm) {
        try {
            return OutlierAlgorithms.valueOf(outlierAlgorithm).newInstance();
        }
        catch(Throwable t) {
            try {
                return (OutlierAlgorithm) OutlierAlgorithm.class.forName(outlierAlgorithm).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to instantiate " + outlierAlgorithm + " or find it in the OutlierAlgorithms enum", e);
            }
        }
    }
}
