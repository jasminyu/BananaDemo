package com.etisalat.log.sort;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "schema")
@XmlAccessorType(XmlAccessType.FIELD)
public class Schema {

    @XmlElement(name = "field")
    private List<Field> fields;

    @XmlElement(name = "uniqueKey")
    private String uniqueKey;

    public Schema() {
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }
}

