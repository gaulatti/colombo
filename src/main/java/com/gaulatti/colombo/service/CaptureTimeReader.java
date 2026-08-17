package com.gaulatti.colombo.service;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import java.io.File;
import java.time.Instant;
import java.util.Date;
import org.springframework.stereotype.Component;

/** Reads the original EXIF capture timestamp when it is present. */
@Component
public class CaptureTimeReader {
    public Instant read(File file) {
        try {
            Metadata metadata = ImageMetadataReader.readMetadata(file);
            ExifSubIFDDirectory directory = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
            Date captured = directory == null ? null : directory.getDateOriginal();
            return captured == null ? null : captured.toInstant();
        } catch (Exception ignored) {
            return null;
        }
    }
}
