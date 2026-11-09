import Foundation

/// The relay's acknowledgement of one publish. It is kept from the moment this is created, so an
/// acknowledgement that comes in before anyone waits for it is not lost, and the wait ends exactly once:
/// at the acknowledgement, or with `RelayError.requestTimeout` when none came in time.
///
/// Combine's `timeout` on `RelayClient`'s concurrent queue can't do this: it delivers values and
/// completions on that queue, where they can run out of order, and a value delivered after the
/// completion is dropped.
final class PublishAcknowledgement {
    private let lock = NSLock()
    private var isAcknowledged = false
    private var waiter: UnsafeContinuation<Void, Error>?

    /// The relay acknowledged the publish.
    func acknowledge() {
        lock.lock()
        isAcknowledged = true
        let waiter = self.waiter
        self.waiter = nil
        lock.unlock()
        waiter?.resume()
    }

    /// Returns once the publish is acknowledged, at once if it already is, or throws
    /// `RelayError.requestTimeout` when it isn't within `timeout`. Cancellation doesn't end the wait.
    func wait(timeout: DispatchTimeInterval, on queue: DispatchQueue) async throws {
        try await withUnsafeThrowingContinuation { (continuation: UnsafeContinuation<Void, Error>) in
            lock.lock()
            if isAcknowledged {
                lock.unlock()
                continuation.resume()
                return
            }
            waiter = continuation
            lock.unlock()
            queue.asyncAfter(deadline: .now() + timeout) { [self] in
                lock.lock()
                let waiter = self.waiter
                self.waiter = nil
                lock.unlock()
                waiter?.resume(throwing: RelayError.requestTimeout)
            }
        }
    }
}
