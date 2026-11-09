
import Foundation

class SessionResponder {
    enum Errors: Error {
        case sessionRequestExpired
    }
    private let logger: ConsoleLogging
    private let sessionStore: WCSessionStorage
    private let networkingInteractor: NetworkInteracting
    private let verifyContextStore: CodableStore<VerifyContext>
    private let sessionRequestsProvider: SessionRequestsProvider
    private let historyService: HistoryService

    init(logger: ConsoleLogging, sessionStore: WCSessionStorage, networkingInteractor: NetworkInteracting, verifyContextStore: CodableStore<VerifyContext>, sessionRequestsProvider: SessionRequestsProvider, historyService: HistoryService) {
        self.logger = logger
        self.sessionStore = sessionStore
        self.networkingInteractor = networkingInteractor
        self.verifyContextStore = verifyContextStore
        self.sessionRequestsProvider = sessionRequestsProvider
        self.historyService = historyService
    }

    func respondSessionRequest(topic: String, requestId: RPCID, response: RPCResult) async throws {
        guard sessionStore.hasSession(forTopic: topic) else {
            throw WalletConnectError.noSessionMatchingTopic(topic)
        }

        let protocolMethod = SessionRequestProtocolMethod()

        guard sessionRequestNotExpired(requestId: requestId) else {
            // Show the request queued behind this one, even if publishing the error fails.
            defer { sessionRequestsProvider.emitRequestIfPendingAfterDelay() }
            try await networkingInteractor.respondError(
                topic: topic,
                requestId: requestId,
                protocolMethod: protocolMethod,
                reason: SignReasonCode.sessionRequestExpired
            )
            verifyContextStore.delete(forKey: requestId.string)
            throw Errors.sessionRequestExpired
        }

        do {
            try await networkingInteractor.respond(
                topic: topic,
                response: RPCResponse(id: requestId, outcome: response),
                protocolMethod: protocolMethod
            )
        } catch RelayError.requestTimeout {
            // The answer went out but its acknowledgement timed out, and respond resolved the request:
            // show the request queued behind it, as after an acknowledged answer, then report the timeout.
            verifyContextStore.delete(forKey: requestId.string)
            sessionRequestsProvider.emitRequestIfPendingAfterDelay()
            throw RelayError.requestTimeout
        }
        verifyContextStore.delete(forKey: requestId.string)
        sessionRequestsProvider.emitRequestIfPendingAfterDelay()
    }

    private func sessionRequestNotExpired(requestId: RPCID) -> Bool {
        guard let request = historyService.getSessionRequest(id: requestId)?.request
        else { return false }

        return !request.isExpired()
    }
}
