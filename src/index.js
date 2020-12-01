const Queue = require('bull')

class Broadcaster {
    constructor(bot, bullQueueOptions) {
        this.queue = new Queue('broadcast', bullQueueOptions)
        this.queue.process((job, done) => {
            const { userId, chatId, fromChatId, messageId, messageText, extra } = job.data

            console.log(job.data)
            if (messageId) {
                return bot.telegram.callApi('copyMessage', {
                    chat_id: userId,
                    from_chat_id: fromChatId || chatId,
                    message_id: messageId,
                    ...extra,
                })
                    .then(() => done())
                    .catch(done)
            }

            return bot.telegram.sendMessage(userId, messageText, extra)
                .then(() => done())
                .catch(done)
        })
    }

    broadcast(chatId, userIds, message, extra) {
        let jobData = message

        if (typeof message === 'string') {
            jobData = {
                messageText: message,
                extra,
            }
        } else if (typeof message === 'number') {
            jobData = {
                chatId: chatId,
                messageId: message,
                extra,
            }
        }

        userIds.forEach(userId => this.queue.add({ userId, ...jobData }))
    }

    reset() {
        const queue = this.queue

        return Promise.all([
            queue.empty(),
            queue.clean(0, 'delayed'),
            queue.clean(0, 'wait'),
            queue.clean(0, 'active'),
            queue.clean(0, 'completed'),
            queue.clean(0, 'failed'),
        ])
    }

    terminate() {
        const queue = this.queue

        return Promise.all([
            queue.empty(),
            queue.clean(0, 'wait'),
            queue.clean(0, 'active'),
        ])
    }

    pause() {
        return this.queue.pause()
    }

    resume() {
        return this.queue.resume()
    }

    failed() {
        return this.queue.getFailed()
    }

    async status() {
        const queue = this.queue

        return {
            failedCount: await queue.getFailedCount(),
            completedCount: await queue.getCompletedCount(),
            activeCount: await queue.getActiveCount(),
            delayedCount: await queue.getDelayedCount(),
            waitingCount: await queue.getWaitingCount(),
        }
    }
}

module.exports = Broadcaster
