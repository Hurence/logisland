package com.hurence.logisland.util.stream.io;

import java.io.IOException;

public class BytePatternNotFoundException extends IOException {

    private static final long serialVersionUID = -4128911284318513973L;

    public BytePatternNotFoundException(final String explanation) {
        super(explanation);
    }
}
