package com.palominolabs.benchpress.example.sumofsquares;

import com.palominolabs.benchpress.job.task.ComponentFactory;
import com.palominolabs.benchpress.job.task.TaskFactory;
import com.palominolabs.benchpress.job.task.TaskOutputProcessorFactory;
import com.palominolabs.benchpress.job.task.TaskOutputQueueProvider;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class SumOfSquaresComponentFactory implements ComponentFactory {
    private final SumOfSquaresWorkerConfig config;

    SumOfSquaresComponentFactory(SumOfSquaresWorkerConfig config) {
        this.config = config;
    }

    @Nonnull
    @Override
    public TaskFactory getTaskFactory() {
        return new TaskFactory() {
            @Nonnull
            @Override
            public Collection<Runnable> getRunnables(@Nonnull UUID jobId, int partitionId,
                    @Nonnull UUID workerId,
                    @Nonnull TaskOutputQueueProvider taskOutputQueueProvider,
                    @Nullable TaskOutputProcessorFactory taskOutputProcessorFactory) throws IOException {
                return Collections.singleton(() -> {
                    long sum = 0;
                    for (int i = config.first; i <= config.last; i++) {
                        sum += i * i;
                    }

                    // TODO expose sum
                });
            }

            @Override
            public void shutdown() {
                // no op
            }
        };
    }

    @Nullable
    @Override
    public TaskOutputProcessorFactory getTaskOutputProcessorFactory() {
        return null;
    }
}
