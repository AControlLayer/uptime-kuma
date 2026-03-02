const { R } = require("redbean-node");
const { log, DOWN } = require("../src/util");

/**
 * Delayed notification queue for cascade suppression.
 *
 * When a child monitor with suppressOnParentDown=true goes DOWN,
 * the notification is held for a delay period. After the delay,
 * the parent's latest heartbeat is checked. If the parent is also
 * DOWN, the child notification is suppressed. Otherwise it sends.
 */
class NotificationQueue {

    constructor() {
        /** @type {Map<number, {timer: NodeJS.Timeout, monitor: object, bean: object, isFirstBeat: boolean}>} */
        this.pending = new Map();
    }

    /**
     * Enqueue a notification for delayed sending with parent check.
     * Only one pending notification per monitor — newer replaces older.
     *
     * @param {object} monitor The child monitor instance
     * @param {object} bean The heartbeat bean
     * @param {boolean} isFirstBeat Whether this is the first beat
     * @param {Function} sendFn The function to call if notification should send: (isFirstBeat, monitor, bean) => Promise<void>
     * @returns {void}
     */
    enqueue(monitor, bean, isFirstBeat, sendFn) {
        // Cancel any existing pending notification for this monitor
        if (this.pending.has(monitor.id)) {
            clearTimeout(this.pending.get(monitor.id).timer);
            this.pending.delete(monitor.id);
        }

        // Calculate delay: parent's interval + 5s buffer, minimum 10s
        const parentInterval = monitor._parentInterval || 60;
        const delayMs = Math.max(10000, parentInterval * 1000 + 5000);

        log.info("notification-queue", `[${monitor.name}] Queuing notification for ${delayMs}ms (parent interval: ${parentInterval}s)`);

        const timer = setTimeout(async () => {
            this.pending.delete(monitor.id);
            await this.resolve(monitor, bean, isFirstBeat, sendFn);
        }, delayMs);

        // Don't let the timer prevent Node.js from exiting
        if (timer.unref) {
            timer.unref();
        }

        this.pending.set(monitor.id, { timer, monitor, bean, isFirstBeat, sendFn });
    }

    /**
     * Resolve a pending notification by checking parent status.
     *
     * @param {object} monitor The child monitor
     * @param {object} bean The heartbeat bean
     * @param {boolean} isFirstBeat Whether this is the first beat
     * @param {Function} sendFn The send function
     * @returns {Promise<void>}
     */
    async resolve(monitor, bean, isFirstBeat, sendFn) {
        try {
            const parentHeartbeat = await R.getRow(
                "SELECT status FROM heartbeat WHERE monitor_id = ? ORDER BY time DESC LIMIT 1",
                [monitor.parent]
            );

            if (parentHeartbeat && parentHeartbeat.status === DOWN) {
                // Parent is DOWN — suppress this notification
                const parentRow = await R.getRow("SELECT name FROM monitor WHERE id = ?", [monitor.parent]);
                const parentName = parentRow ? parentRow.name : `#${monitor.parent}`;
                log.info("notification-queue", `[${monitor.name}] Notification SUPPRESSED: parent [${parentName}] is DOWN`);
                return;
            }

            // Parent is UP or PENDING or no heartbeat — send the notification
            log.info("notification-queue", `[${monitor.name}] Parent is UP, sending delayed notification`);
            await sendFn(isFirstBeat, monitor, bean);
        } catch (e) {
            log.error("notification-queue", `[${monitor.name}] Error resolving notification: ${e.message}`);
            // On error, send the notification rather than silently suppress
            try {
                await sendFn(isFirstBeat, monitor, bean);
            } catch (sendErr) {
                log.error("notification-queue", `[${monitor.name}] Failed to send notification: ${sendErr.message}`);
            }
        }
    }

    /**
     * Flush all pending notifications immediately (used on shutdown).
     * Sends all queued notifications without checking parent status.
     *
     * @returns {Promise<void>}
     */
    async flush() {
        log.info("notification-queue", `Flushing ${this.pending.size} pending notification(s)`);
        const entries = Array.from(this.pending.values());
        this.pending.clear();

        for (const entry of entries) {
            clearTimeout(entry.timer);
            try {
                await entry.sendFn(entry.isFirstBeat, entry.monitor, entry.bean);
            } catch (e) {
                log.error("notification-queue", `Failed to flush notification for [${entry.monitor.name}]: ${e.message}`);
            }
        }
    }

    /**
     * Get the number of pending notifications.
     * @returns {number}
     */
    get size() {
        return this.pending.size;
    }
}

// Singleton instance
const notificationQueue = new NotificationQueue();

module.exports = {
    notificationQueue,
    NotificationQueue,
};
