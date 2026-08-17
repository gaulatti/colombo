package com.gaulatti.colombo.ftp;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Versioned, CMS-owned upload naming policy. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UploadNamingPolicy {
    private int version;
    private String assignmentSlug;
    private List<UploadNamingSegment> path;
    private List<UploadNamingSegment> filename;
    private String timezone;
    private String captureTimeFallback;
    private String caseMode;

    public boolean isValid() {
        return version == 1
                && assignmentSlug != null && !assignmentSlug.isBlank()
                && path != null
                && filename != null && !filename.isEmpty()
                && filename.stream().anyMatch(segment ->
                    "placeholder".equals(segment.getType()) && "sequence".equals(segment.getName()))
                && timezone != null && !timezone.isBlank()
                && ("uploadedTime".equals(captureTimeFallback) || "reject".equals(captureTimeFallback))
                && ("preserve".equals(caseMode) || "lowercase".equals(caseMode) || "uppercase".equals(caseMode));
    }
}
