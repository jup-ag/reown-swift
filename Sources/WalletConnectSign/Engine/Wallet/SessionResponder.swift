
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
            do {
                try await networkingInteractor.respondError(
                    topic: topic,
                    requestId: requestId,
                    protocolMethod: protocolMethod,
                    reason: SignReasonCode.sessionRequestExpired
                )
            } catch RelayError.requestTimeout {
                // The error went out: settle the request, as after a timed-out answer below.
                settleAnsweredRequest(requestId)
                throw RelayError.requestTimeout
            }
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
            // The answer went out but its acknowledgement timed out, so the dapp may have it and the
            // wallet can't answer again. respond resolves the record only once the relay acknowledges,
            // so settle the request here, or it stays pending and holds back every request queued
            // behind it. Then show the next one, as after an acknowledged answer, and report the timeout.
            settleAnsweredRequest(requestId)
            sessionRequestsProvider.emitRequestIfPendingAfterDelay()
            throw RelayError.requestTimeout
        }
        verifyContextStore.delete(forKey: requestId.string)
        sessionRequestsProvider.emitRequestIfPendingAfterDelay()
    }

    /// Settles a session request whose answer may have reached the dapp: deletes its record, as
    /// respond's resolve does once the relay acknowledges, and its verify context. Only session
    /// requests queue behind one another, so only they are settled when an acknowledgement times
    /// out: any other request keeps its record and can be answered again.
    private func settleAnsweredRequest(_ requestId: RPCID) {
        historyService.removeAnsweredRequest(id: requestId)
        verifyContextStore.delete(forKey: requestId.string)
    }

    private func sessionRequestNotExpired(requestId: RPCID) -> Bool {
        guard let request = historyService.getSessionRequest(id: requestId)?.request
        else { return false }

        return !request.isExpired()
    }
}
