package com.hurence.logisland.utils;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

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

    public void updateList() {
        updateLock.lock();
        try {
            if (fileQueue.size() < 20) {
                java.nio.file.Files.find(Paths.get(inputPath.getAbsolutePath()),
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
                        .limit(20)
                        .forEach(f -> {
                            File newFile = f.toFile();
                            fileQueue.add(newFile);
                        });
            }
        } catch (IOException e) {
            log.error("error in upadteList()", e);
        } finally {
            updateLock.unlock();
        }
    }

    public void closeAndMoveToFinished(InputStream inputStream, File inputFile, File inputDirectory, File outputDirectory, boolean errored) throws IOException {
        updateLock.lock();
        try {
            if (null != inputStream) {
                log.info("Closing {}", inputFile);
                inputStream.close();

                String enclosingFolderName = inputFile.getAbsolutePath()
                        .replaceAll(inputDirectory.getAbsolutePath(), "")
                        .replaceAll(inputFile.getName(), "");

                File realOutputDir = new File(outputDirectory.getAbsolutePath() + enclosingFolderName);
                if (!realOutputDir.exists())
                    realOutputDir.mkdirs();

                File finishedFile = new File(realOutputDir, inputFile.getName());

                if (errored) {
                    log.error("Error during processing, moving {} to {}.", inputFile, outputDirectory);
                }

                if (inputFile.exists()) {
                    Files.move(inputFile, finishedFile);
                } else {
                    log.trace("Unable to move file {}, may be already moved.", inputFile);
                }

                File processingFile = getProcessingFile(inputFile);
                if (processingFile.exists()) {
                    log.info("Removing processing file {}", processingFile);
                    processingFile.delete();
                }

            }
        } finally {
            updateLock.unlock();
        }

    }

    public File take() {
        updateLock.lock();

        File file = null;
        try {
            file = fileQueue.poll();
            if (file != null) {
                File processingFile = getProcessingFile(file);
                Files.touch(processingFile);
            }
        } catch (IOException e) {
            log.error("error in take()", e);
        } finally {
            updateLock.unlock();
        }

        return file;
    }

    File getProcessingFile(File input) {
        String fileName = input.getName() + processingFileExtension;
        return new File(input.getParentFile(), fileName);
    }
}
