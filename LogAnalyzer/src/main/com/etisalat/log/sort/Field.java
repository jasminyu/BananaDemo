package com.etisalat.log.sort;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "field")
@XmlAccessorType(XmlAccessType.FIELD)
public class Field {

    @XmlAttribute
    private String name;
    @XmlAttribute
    private SortField.Type type;

    public Field() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SortField.Type getType() {
        return type;
    }

    public void setType(SortField.Type type) {
        this.type = type;
    }
}
