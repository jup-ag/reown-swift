
import Foundation
import Combine

class RequestsExpiryWatcher {

    private let requestExpirationPublisherSubject: PassthroughSubject<RPCID, Never> = .init()
    private let rpcHistory: RPCHistory
    private let historyService: HistoryService
    private let sessionRequestsProvider: SessionRequestsProvider

    var requestExpirationPublisher: AnyPublisher<RPCID, Never> {
        return requestExpirationPublisherSubject.eraseToAnyPublisher()
    }

    private var checkTimer: Timer?

    /// `startsCheckTimer` is false only in tests, which call `checkForRequestExpiry()` themselves.
    internal init(
        proposalPayloadsStore: CodableStore<RequestSubscriptionPayload<SessionType.ProposeParams>>,
        rpcHistory: RPCHistory,
        historyService: HistoryService,
        sessionRequestsProvider: SessionRequestsProvider,
        startsCheckTimer: Bool = true
    ) {
        self.rpcHistory = rpcHistory
        self.historyService = historyService
        self.sessionRequestsProvider = sessionRequestsProvider
        if startsCheckTimer {
            setUpExpiryCheckTimer()
        }
    }

    deinit {
        // A timer is invalidated on the thread that installed it, the main thread here.
        let timer = checkTimer
        if Thread.isMainThread {
            timer?.invalidate()
        } else {
            DispatchQueue.main.async { timer?.invalidate() }
        }
    }

    func setUpExpiryCheckTimer() {
        checkTimer = Timer.scheduledTimer(withTimeInterval: 3.0, repeats: true) { [weak self] _ in
            self?.checkForRequestExpiry()
        }
    }

    func checkForRequestExpiry() {
        let requests = historyService.getPendingExpirableRequestsWithId()
        var deletedAny = false
        requests.forEach { (request: Expirable, recordId: RPCID) in
            guard request.isExpired() else { return }
            requestExpirationPublisherSubject.send(recordId)
            rpcHistory.delete(id: recordId)
            deletedAny = true
        }
        // Otherwise the request queued behind an expired one is emitted only when another arrives.
        if deletedAny {
            sessionRequestsProvider.emitRequestIfPendingAfterExpiry()
        }
    }

}
