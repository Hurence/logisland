package com.hurence.logisland.utils;

import com.google.common.io.Files;
import com.sun.management.UnixOperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class SynchronizedFileLister {

    /**
     * Lock held by take, poll, etc
     */
    private final ReentrantLock updateLock = new ReentrantLock();

    private File inputPath;
    private FilenameFilter inputFilenameFilter;
    private String processingFileExtension;
    private long minimumFileAgeMS;

    private final BlockingQueue<File> fileQueue = new LinkedHashSetBlockingQueue<>(1024);


    private static final Logger log = LoggerFactory.getLogger(SynchronizedFileLister.class);


    /**
     * Holder
     */
    private static class SynchronizedFileListerHolder {
        /**
         * Instance unique non préinitialisée
         */
        private final static SynchronizedFileLister instance = new SynchronizedFileLister();
    }

    /**
     * Point d'accès pour l'instance unique du singleton
     */
    public static SynchronizedFileLister getInstance(File inputPath,
                                                     FilenameFilter inputFilenameFilter,
                                                     long minimumFileAgeMS,
                                                     String processingFileExtension) {
        return SynchronizedFileListerHolder.instance
                .config(inputPath, inputFilenameFilter, minimumFileAgeMS, processingFileExtension);
    }

    private SynchronizedFileLister config(File inputPath,
                                          FilenameFilter inputFilenameFilter,
                                          long minimumFileAgeMS,
                                          String processingFileExtension) {
        this.inputPath = inputPath;
        this.inputFilenameFilter = inputFilenameFilter;
        this.minimumFileAgeMS = minimumFileAgeMS;
        this.processingFileExtension = processingFileExtension;

        return this;
    }

    public void unlock() {
        updateLock.unlock();
    }

    public void lock() {
        updateLock.lock();
    }

    public boolean needToUpdateList() {
        return fileQueue.size() < 20;
    }

    /**
     * Fill up queue with files to be processed
     * Does not create the processing file.
     */
    public void updateList() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if(log.isTraceEnabled() && os instanceof UnixOperatingSystemMXBean){
            log.trace("before updateList(), number of open file descriptors is {}", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        }
        log.debug("before updateList(), size of file queue is {}", fileQueue.size());
        try (Stream<Path> pathsToQueue = getFilesStreamToQueue()) {//so that stream is closed at end (to releases file ressources)
            pathsToQueue.forEach(f -> {
                File newFile = f.toFile();
                fileQueue.add(newFile);
            });
        } catch (IOException e) {
            log.error("error in updateList()", e);
        }
        if(log.isTraceEnabled() && os instanceof UnixOperatingSystemMXBean){
            log.trace("after updateList(), number of open file descriptors is {}", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        }
        log.debug("after updateList(), size of file queue is {}", fileQueue.size());
    }

    public Stream<Path> getFilesStreamToQueue() throws IOException {
        return java.nio.file.Files.find(Paths.get(inputPath.getAbsolutePath()),
                10,
                (filePath, fileAttr) -> fileAttr.isRegularFile() &&
                        !filePath.toUri().getPath().contains(processingFileExtension) &&
                        inputFilenameFilter.accept(filePath.getParent().toFile(), filePath.getFileName().toString()))
                .filter(f -> {
                    File newFile = f.toFile();
                    long fileAgeMS = System.currentTimeMillis() - newFile.lastModified();
                    if (fileAgeMS < 0L) {
                        log.error("File {} has a date in the future.", newFile);
                    }
                    File processingFile = getProcessingFile(newFile);
                    if (processingFile.exists()) {
                        log.trace("Skipping file {} because a processing file already exists.", newFile);
                        return false;
                    } else if (minimumFileAgeMS > 0L && fileAgeMS < minimumFileAgeMS) {
                        log.debug("Skipping file {} because it does not meet the minimum age.", newFile);
                        return false;
                    } else {
                        return true;
                    }
                })
                .limit(200);
    }

    public void moveTo(File inputFile, File inputDirectory, File outputDirectory) throws IOException {
//        updateLock.lock();
//        try {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if(log.isTraceEnabled() && os instanceof UnixOperatingSystemMXBean){
            log.trace("before moveTo(), number of open file descriptors is {}", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        }
        String enclosingFolderName = inputFile.getAbsolutePath()
                .replaceAll(inputDirectory.getAbsolutePath(), "")
                .replaceAll(inputFile.getName(), "");

        File realOutputDir = new File(outputDirectory.getAbsolutePath() + enclosingFolderName);
        if (!realOutputDir.exists())
            realOutputDir.mkdirs();


        File finishedFile = new File(realOutputDir, inputFile.getName());

        if (inputFile.exists()) {
            Files.move(inputFile, finishedFile);
        } else {
            log.warn("file {} does not exist anymore (should not happen if there is only one VM).", inputFile);
        }

        File processingFile = getProcessingFile(inputFile);
        if (processingFile.exists()) {
            processingFile.delete();
            log.info("Deleted processing file {}", processingFile);
        } else {
            log.warn("Processing file {} is already deleted (should not happen if there is only one VM).",
                    processingFile);
        }
        if(log.isTraceEnabled() && os instanceof UnixOperatingSystemMXBean){
            log.trace("after moveTo(), number of open file descriptors is {}", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
        }
//        } finally {
//            updateLock.unlock();
//        }
    }

    /**
     * if it return a files, it creates the processing file in the process.
     * If this creation fail then it return null.
     * @return the next file in queue (can be null)
     */
    public File take() {
        updateLock.lock();
        log.debug("before take(), size of file queue is {}", fileQueue.size());
        File file = null;
        try {
            file = fileQueue.poll();
            if (file != null) {
                File processingFile = getProcessingFile(file);
                if (processingFile.createNewFile()) {
                    return file;
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            log.error("error in take()", e);
        } finally {
            log.debug("after take(), size of file queue is {}", fileQueue.size());
            updateLock.unlock();
        }

        return file;
    }

    File getProcessingFile(File input) {
        String fileName = input.getName() + processingFileExtension;
        return new File(input.getParentFile(), fileName);
    }
}
