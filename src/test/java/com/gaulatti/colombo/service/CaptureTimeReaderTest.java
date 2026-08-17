package com.gaulatti.colombo.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import java.io.File;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class CaptureTimeReaderTest {
    private final CaptureTimeReader reader = new CaptureTimeReader();
    private final File file = new File("photo.jpg");

    @Test
    void returnsExifCaptureTime() throws Exception {
        Metadata metadata = mock(Metadata.class);
        ExifSubIFDDirectory directory = mock(ExifSubIFDDirectory.class);
        Date date = Date.from(Instant.parse("2026-08-17T14:35:22Z"));
        when(metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class)).thenReturn(directory);
        when(directory.getDateOriginal()).thenReturn(date);
        try (MockedStatic<ImageMetadataReader> image = mockStatic(ImageMetadataReader.class)) {
            image.when(() -> ImageMetadataReader.readMetadata(file)).thenReturn(metadata);
            assertEquals(date.toInstant(), reader.read(file));
        }
    }

    @Test
    void returnsNullForMissingDirectoryDateAndReadFailure() throws Exception {
        Metadata metadata = mock(Metadata.class);
        try (MockedStatic<ImageMetadataReader> image = mockStatic(ImageMetadataReader.class)) {
            image.when(() -> ImageMetadataReader.readMetadata(file)).thenReturn(metadata);
            assertNull(reader.read(file));
        }

        ExifSubIFDDirectory directory = mock(ExifSubIFDDirectory.class);
        when(metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class)).thenReturn(directory);
        try (MockedStatic<ImageMetadataReader> image = mockStatic(ImageMetadataReader.class)) {
            image.when(() -> ImageMetadataReader.readMetadata(file)).thenReturn(metadata);
            assertNull(reader.read(file));
        }

        try (MockedStatic<ImageMetadataReader> image = mockStatic(ImageMetadataReader.class)) {
            image.when(() -> ImageMetadataReader.readMetadata(file)).thenThrow(new RuntimeException("bad image"));
            assertNull(reader.read(file));
        }
    }
}
