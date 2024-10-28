package com.example.CodeChef;

import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class TaskExecutorImpl implements Main.TaskExecutor {

    private final ExecutorService executorService;
    private final Map<UUID, Lock> taskLockMap = new ConcurrentHashMap<>();
    private final Queue<Runnable> queue = new LinkedList<>();
    private final Object taskLock = new Object();
    private final int maxConcurrentTasks;

    public TaskExecutorImpl() {
        this.executorService = Executors.newCachedThreadPool();
        this.maxConcurrentTasks = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task.taskAction());

        synchronized (taskLock) {
            taskLockMap.putIfAbsent(task.taskGroup().groupUUID(), new ReentrantLock());
            queue.add(() -> {
                Lock groupLock = taskLockMap.get(task.taskGroup().groupUUID());
                groupLock.lock();
                try {
                    futureTask.run();
                } finally {
                    groupLock.unlock();
                    synchronized (taskLock) {
                        queue.poll();
                    }
                }
            });
            processTaskQueue();
        }
        return futureTask;
    }

    private void processTaskQueue() {
        while (!queue.isEmpty() && ((ThreadPoolExecutor) executorService).getActiveCount() < maxConcurrentTasks) {
            Runnable nextTask = queue.peek();
            if (nextTask != null) {
                executorService.submit(nextTask);
            }
        }
    }
}
