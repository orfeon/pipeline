package com.mercari.solution.util.domain.file;

import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipFileUtil {

    private static final String CONTENT_TYPE_ZIP = "application/zip";

    private static final Logger LOG = LoggerFactory.getLogger(ZipFileUtil.class);

    public static void writeZipFile(final OutputStream outputStream, final String localDirPath) throws IOException {
        writeZipFile(outputStream, localDirPath, localDirPath);
    }

    public static void writeZipFile(final OutputStream outputStream, final String localDirPath, final String targetDirPath) throws IOException {
        final Path indexDirPath = Paths.get(targetDirPath);
        try (final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(outputStream))) {
            zipFile(zos, indexDirPath.toFile(), localDirPath);
            zos.flush();
        }
    }

    public static void uploadZipFile(final String localDirPath, final String gcsPath) throws IOException {
        final Path indexDirPath = Paths.get(localDirPath);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
            zipFile(zos, indexDirPath.toFile(), localDirPath);
            zos.flush();
            baos.flush();
        }

        baos.close();
        StorageUtil.writeBytes(gcsPath, baos.toByteArray(), CONTENT_TYPE_ZIP, new HashMap<>(), new HashMap<>());
    }

    public static void downloadZipFiles(final String gcsPath, final String localDirPath) throws IOException {
        final File coreDir = new File(localDirPath);
        if(!coreDir.exists() || coreDir.isFile()) {
            coreDir.mkdirs();
        }
        try(final InputStream is = StorageUtil.readStream(StorageUtil.storage(), gcsPath);
            final BufferedInputStream bis = new BufferedInputStream(is);
            final ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry zipEntry;
            while((zipEntry = zis.getNextEntry()) != null) {
                unzipFile(zis, zipEntry, localDirPath);
                zis.closeEntry();
            }
        } catch (EOFException e) {
            LOG.error("failed to download: " + gcsPath + " to local: " + localDirPath + " cause: " + e.getMessage());
        }
    }

    private static void zipFile(final ZipOutputStream zos, final File file, final String dirPath) throws IOException {
        if(file.isDirectory()) {
            for(final File childFile : file.listFiles()) {
                zipFile(zos, childFile, dirPath);
            }
            return;
        }
        final String fileName = file.getAbsolutePath().replaceFirst(dirPath, "");
        final ZipEntry zipEntry = new ZipEntry(fileName);
        zos.putNextEntry(zipEntry);
        try(final InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            LOG.info("zip: " + file.getAbsolutePath());
            int len;
            byte[] buf = new byte[1024 * 1024];
            while ((len = is.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
        }
    }

    private static void unzipFile(final ZipInputStream zis, final ZipEntry zipEntry, final String dirPath) throws IOException {
        final File file = new File(dirPath + zipEntry.getName());
        if (zipEntry.isDirectory()) {
            if(!file.exists()) {
                file.mkdirs();
            }
            return;
        }
        if(!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        try (final FileOutputStream fos = new FileOutputStream(file);
             final BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            LOG.info("unzip: " + file.getAbsolutePath());
            int len;
            byte[] buf = new byte[1024];
            while ((len = zis.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }
            bos.flush();
        }
    }

    public static <T> FileIO.Write<String, T> createSingleFileWrite(
            final String output,
            final List<String> groupFields,
            final String tempDirectory,
            final SerializableFunction<T, String> destinationFunction) {

        FileIO.Write<String, T> write;
        if(groupFields.size() > 0) {
            write = FileIO.<String, T>writeDynamic()
                    .to(output)
                    .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                    .withDestinationCoder(StringUtf8Coder.of());
        } else {
            final String outdir = StorageUtil.removeDirSuffix(output);
            write = FileIO.<String, T>writeDynamic()
                    .to(outdir)
                    .by(d -> "")
                    .withDestinationCoder(StringUtf8Coder.of());
        }

        final String filename = StorageUtil.addFilePrefix(output, "");
        write = write
                .withNumShards(1)
                .withNoSpilling()
                .withNaming(key -> TemplateFileNaming.of(filename, key));

        if(tempDirectory != null) {
            write = write.withTempDirectory(tempDirectory);
        }

        return write;
    }

    public static class TemplateFileNaming implements FileIO.Write.FileNaming {

        private static final DateTimeUtil.DateTimeTemplateUtils datetimeUtils = new DateTimeUtil.DateTimeTemplateUtils();

        private final String template;
        private final String key;

        private TemplateFileNaming(final String template, final String key) {
            this.template = template;
            this.key = key;
        }

        @Override
        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            return createFileName(template, key, window, pane, numShards, shardIndex);
        }

        private static String createFileName(final String templateString,
                                             final String key,
                                             final BoundedWindow window,
                                             final PaneInfo pane,
                                             final int numShards,
                                             final int shardIndex) {

            final Instant windowStart;
            final Instant windowEnd;
            if(window != GlobalWindow.INSTANCE) {
                if(!(window instanceof IntervalWindow)) {
                    throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                            + window.getClass().getSimpleName());
                }
                final IntervalWindow iw = ((IntervalWindow)window);
                windowStart = Instant.ofEpochMilli(iw.start().getMillis());
                windowEnd   = Instant.ofEpochMilli(iw.end().getMillis());
            } else {
                windowStart = Instant.ofEpochMilli(0L);
                windowEnd   = Instant.ofEpochMilli(4102444800L);
            }

            // Pane Path
            final long paneIndex;
            if(!pane.isFirst() || !pane.isLast()) {
                paneIndex = pane.getIndex();
            } else {
                paneIndex = 0;
            }

            final Map<String, Object> map = new HashMap<>();
            map.put("_DateTimeUtil", datetimeUtils);

            map.put("__KEY__", key);
            map.put("__NUM_SHARDS__", numShards);
            map.put("__SHARD_INDEX__", shardIndex);
            map.put("__WINDOW_START__", windowStart);
            map.put("__WINDOW_END__", windowEnd);
            map.put("__PANE_INDEX__", paneIndex);

            final StringWriter writer = new StringWriter();
            try {
                final Template template = TemplateUtil.createSafeTemplate("templateFileNaming", templateString);
                template.process(map, writer);
                return writer.toString();
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to create filename: " + templateString);
            }
        }

        public static TemplateFileNaming of(final String key) {
            return new TemplateFileNaming(key, key);
        }

        public static TemplateFileNaming of(final String template, final String key) {
            return new TemplateFileNaming(template, key);
        }

    }

}
