package com.cowave.zoo.schedule.fom;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author shanhm1991@163.com
 *
 */
class DelayedTask implements Delayed {

    private final long delayTime;

	public DelayedTask(long delayTime) {
        this.delayTime = System.currentTimeMillis() + delayTime;
    }

    @Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(delayTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public int compareTo(Delayed delayTask) {
		DelayedTask d = (DelayedTask) delayTask;
        return (int) (this.getDelay(TimeUnit.MILLISECONDS) - d.getDelay(TimeUnit.MILLISECONDS));
	}
}
