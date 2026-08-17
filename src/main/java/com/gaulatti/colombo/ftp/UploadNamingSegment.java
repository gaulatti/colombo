package com.gaulatti.colombo.ftp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** A validated literal or placeholder segment supplied by the CMS. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UploadNamingSegment {
    private String type;
    private String value;
    private String name;
    private String format;
    private Integer width;
}
