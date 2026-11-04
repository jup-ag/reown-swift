
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

    func setUpExpiryCheckTimer() {
        checkTimer = Timer.scheduledTimer(withTimeInterval: 3.0, repeats: true) { [weak self] _ in
            self?.checkForRequestExpiry()
        }
    }

    func checkForRequestExpiry() {
        let requests = historyService.getPendingExpirableRequestsWithId()
        requests.forEach { (request: Expirable, recordId: RPCID) in
            guard request.isExpired() else { return }
            requestExpirationPublisherSubject.send(recordId)
            rpcHistory.delete(id: recordId)
        }
    }

}
