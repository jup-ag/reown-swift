import Combine
import Foundation

class SessionRequestsProvider {
    typealias Schedule = (_ delay: TimeInterval, _ work: @escaping () -> Void) -> Void

    private let historyService: HistoryServiceProtocol
    private let schedule: Schedule
    private let currentDate: () -> Date
    private var sessionRequestPublisherSubject = PassthroughSubject<(request: Request, context: VerifyContext?), Never>()
    private var lastEmitTime: Date?
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
    /// or expired has settled, so the wallet shows the next one.
    func emitRequestIfPendingAfterDelay() {
        schedule(nextRequestDelay) { [weak self] in
            self?.emitRequestIfPending()
        }
    }

    func emitRequestIfPending() {
        let now = currentDate()
        if let lastEmitTime = lastEmitTime {
            // Check if the time interval since 'lastEmitTime' is less than 'debounceInterval'
            if now.timeIntervalSince(lastEmitTime) < debounceInterval {
                //    print("🚨 Debounce interval not yet elapsed since last emit.")
                return
            } else {
                //   print("✅ Enough time has elapsed since last emit.")
            }
        } else {
            // print("⚠️ 'lastEmitTime' is nil. Proceeding with operation.")
        }

        self.lastEmitTime = now

        if let oldestRequest = self.historyService.getPendingRequestsSortedByTimestamp().first {
            self.sessionRequestPublisherSubject.send(oldestRequest)
        }
    }
}
