import Combine
import Foundation

class SessionRequestsProvider {
    typealias Schedule = (_ delay: TimeInterval, _ work: @escaping () -> Void) -> Void

    private let historyService: HistoryServiceProtocol
    private let schedule: Schedule
    private let currentDate: () -> Date
    private var sessionRequestPublisherSubject = PassthroughSubject<(request: Request, context: VerifyContext?), Never>()
    /// Guards `lastEmitTime` and `lastEmittedId`: arrivals emit on the networking queue, the delayed
    /// emissions on main.
    private let lock = NSLock()
    private var lastEmitTime: Date?
    private var lastEmittedId: RPCID?
    private let debounceInterval: TimeInterval = 0.4
    private let nextRequestDelay: TimeInterval = 1

    public var sessionRequestPublisher: AnyPublisher<(request: Request, context: VerifyContext?), Never> {
        sessionRequestPublisherSubject.eraseToAnyPublisher()
    }

    init(
        historyService: HistoryServiceProtocol,
        schedule: @escaping Schedule = { delay, work in
            DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: work)
        },
        currentDate: @escaping () -> Date = Date.init
    ) {
        self.historyService = historyService
        self.schedule = schedule
        self.currentDate = currentDate
    }

    /// Emits the oldest pending request a second from now, once the request that was just answered
    /// has settled, so the wallet shows the next one.
    func emitRequestIfPendingAfterDelay() {
        schedule(nextRequestDelay) { [weak self] in
            self?.emit(skippingLastEmitted: false)
        }
    }

    /// Emits the oldest pending request a second after the expiry watcher deleted an expired one,
    /// unless it is the request last emitted: when a queued or authenticate request expired behind
    /// the request on screen, or a request arriving in the meantime emitted it, the wallet is
    /// already showing it.
    func emitRequestIfPendingAfterExpiry() {
        schedule(nextRequestDelay) { [weak self] in
            self?.emit(skippingLastEmitted: true)
        }
    }

    func emitRequestIfPending() {
        emit(skippingLastEmitted: false)
    }

    private func emit(skippingLastEmitted: Bool) {
        lock.lock()
        let now = currentDate()
        if let lastEmitTime = lastEmitTime, now.timeIntervalSince(lastEmitTime) < debounceInterval {
            lock.unlock()
            return
        }
        // Debounce only emissions that send something: one that finds nothing pending must not hide
        // a request arriving right after it, which nothing would emit again.
        guard let oldestRequest = historyService.getPendingRequestsSortedByTimestamp().first,
              !(skippingLastEmitted && oldestRequest.request.id == lastEmittedId)
        else {
            lock.unlock()
            return
        }
        lastEmitTime = now
        lastEmittedId = oldestRequest.request.id
        lock.unlock()
        sessionRequestPublisherSubject.send(oldestRequest)
    }
}
