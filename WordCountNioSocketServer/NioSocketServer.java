package com.qzy.spark.streaming.wordcount.nio;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author qzy1773498686@qq.com
 * @since 2019/6/25 5:41 PM
 */
public class NioSocketServer {
    private Set<String> keyFilter = new HashSet<>();
    private String filepath;
    private long timeDelay;
    private int port = 0;
    private volatile boolean running;
    private Pattern chinese = Pattern.compile("[\u4E00-\u9FA5]{2,}");

    public NioSocketServer(String filepath, long timeDelay, int port) {
        this.filepath = filepath;
        this.timeDelay = timeDelay;
        this.port = port;
        this.running = true;
    }

    public void start() throws IOException {
        Selector select = Selector.open();
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(port));
        ssc.configureBlocking(false);
        ssc.register(select, SelectionKey.OP_ACCEPT);
        System.out.println("start NioSocketServer...");
        while (running) {
            int n = select.select(10);
            if (n < 0) {
                System.err.println("no select channel...");
                break;
            }
            if (n > 0) {
                Iterator<SelectionKey> iter = select.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    System.out.println(key.toString() + "\t" + key.isAcceptable() + "\t" + key.isValid() + "\t" + key.isReadable() + "\t" + key.isWritable());
                    // spark straming socketTextStream会不断产生选择器键，
                    // 这里加一个keyFilter是为了读完文件，关闭当前channel即可，不需要重复写。
                    if (keyFilter.contains(key.toString())) {
                        break;
                    }
                    keyFilter.add(key.toString());
                    if (key.isValid() && key.isAcceptable()) {
                        acceptkey(key);
                    } else if (key.isValid() && key.isWritable()) {
                        writekey(key);
                    }
                }
            }
        }
    }

    public void acceptkey(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel channel = ssc.accept();
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.register(key.selector(), SelectionKey.OP_WRITE);
    }

    public void writekey(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Segment seg = HanLP.newSegment().enablePartOfSpeechTagging(true);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filepath), "gbk"));
        String line = null;
        while ((line = br.readLine()) != null) {
            List<Term> terms = seg.seg(line);
            if (terms.isEmpty())
                continue;
            StringBuilder builder = new StringBuilder();
            for (Term t : terms) {
                String word = t.word.trim();
                if (word.length() > 1 && chinese.matcher(word).find()) {
                    builder.append(t.word);
                    builder.append(" ");
                }
            }
            if (builder.length() > 0) {
                builder.append("\n");
                ByteBuffer byteBuf = ByteBuffer.wrap(builder.toString().getBytes("utf-8"));
                channel.write(byteBuf);

                try {
                    Thread.sleep(timeDelay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        br.close();
        channel.close();
    }

    public static void main(String[] args) throws IOException {
        String filepath = NioSocketServer.class.getResource("/39.txt").getPath();
        new NioSocketServer(filepath, 1000L, 9999).start();
    }


}
