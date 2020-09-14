package com.dist.mykafka;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class FileMessageSet {
    private static Logger LOG = Logger.getLogger(FileMessageSet.class);
    private File file;
    private FileChannel logFileChannel;
    private long startPosition;
    private long size;

    public FileMessageSet(File file, FileChannel logFileChannel, long startPosition, long size) {
        this.file = file;
        this.logFileChannel = logFileChannel;
        this.startPosition = startPosition;
        this.size = size;
    }

    public long writeTo(SocketChannel socketChannel) {
        try {
            //zero copy transfer
            var bytesTransferred = logFileChannel.transferTo(startPosition, size, socketChannel);
            LOG.trace("FileMessageSet " + file.getAbsolutePath() + " : bytes transferred : " + bytesTransferred
                    + " bytes requested for transfer : " + size);
            return bytesTransferred;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
