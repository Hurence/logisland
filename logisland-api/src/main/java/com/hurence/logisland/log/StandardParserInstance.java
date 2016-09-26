package com.hurence.logisland.log;

import com.hurence.logisland.components.AbstractConfiguredComponent;

/**
 * Created by tom on 01/07/16.
 */
public class StandardParserInstance extends AbstractConfiguredComponent {

    private final AbstractLogParser parser;

    public StandardParserInstance(AbstractLogParser parser, String id) {
        super(parser, id);
        this.parser = parser;
    }

    public AbstractLogParser getParser() {
        return parser;
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }
}
