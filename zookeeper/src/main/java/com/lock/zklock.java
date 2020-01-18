package com.lock;

import com.sun.xml.internal.ws.api.message.ExceptionHasMessage;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class zklock {
    private ZkClient zkClient;
    private String name;
    private String currentLockPath;
    private CountDownLatch countDownLatch;
    private static final String PATENT_LOCK_PATH = "distribute_lock";
    private static final int MAX_RETEY_TIMES = 3;
    private static final int DEFAULT_WAIT_TIME = 3;
    public zklock(ZkClient zkClient, String name) {
        this.zkClient = zkClient;
        this.name = name;
    }
    public void addLock() {
        if (!zkClient.exists(PATENT_LOCK_PATH)) {
            zkClient.createPersistent(PATENT_LOCK_PATH);
        }
        int count = 0;
        boolean iscompleted = false;
        while (!iscompleted) {
            iscompleted = true;
            try {
                //创建当前目录下的临时有序节点
                currentLockPath = zkClient.createEphemeralSequential(PATENT_LOCK_PATH + "/", System.currentTimeMillis());
            } catch (Exception e) {
                if (count++ < MAX_RETEY_TIMES) {
                    iscompleted = false;
                } else
                    throw  e;
            }
        }
    }
    public void releaseLock() {
        zkClient.delete(currentLockPath);
    }
    //检查是否是最小的节点
    private boolean checkMinNode(String localPath) {
        List<String> children = zkClient.getChildren(PATENT_LOCK_PATH);
        Collections.sort(children);
        int index = children.indexOf(localPath.substring(PATENT_LOCK_PATH.length()+1));

        if (index == 0) {
            if (countDownLatch != null) {
                countDownLatch.countDown();
            }
            return true;
        } else {
            String waitPath = PATENT_LOCK_PATH + "/" + children.get(index-1);
            waitForLock(waitPath, false);
            return false;
        }

    }
    //监听有序序列中的前一个节点
    private void waitForLock(String waitPath, boolean useTime) {
        countDownLatch = new CountDownLatch(1);
        zkClient.subscribeDataChanges(waitPath, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                 checkMinNode(currentLockPath);
            }
        });
        if (!zkClient.exists(waitPath)) {
            return;
        }
        try {
            if (useTime == true)
                countDownLatch.await(DEFAULT_WAIT_TIME, TimeUnit.SECONDS);
            else
                countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        countDownLatch = null;
    }

}
