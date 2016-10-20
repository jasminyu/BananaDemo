package com.etisalat.log.query;

import com.etisalat.log.common.LogQueryException;

public interface RspProcess {
    public String process() throws LogQueryException;

    public String processDisplay() throws LogQueryException;

    public String processExport() throws LogQueryException;

}
