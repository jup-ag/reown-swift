import XCTest
import Combine
import JSONRPC
import WalletConnectKMS
import WalletConnectNetworking
@testable import WalletConnectRelay
@testable import WalletConnectSign
@testable import WalletConnectUtils
@testable import TestingUtils

/// A wallet is shown one session request at a time: the oldest pending one, emitted when a request
/// arrives and a second after a request is answered. When the request on screen expires, or its
/// answer's acknowledgement times out, the one queued behind it must be emitted too, or it waits
/// until some other request arrives; and an expiry must not send the wallet the request it is
/// already showing.
///
/// The provider's schedule and clock are faked: a delayed emission runs by hand at the time it was
/// due, so nothing waits on a real clock.
final class NextRequestAfterExpiryTests: XCTestCase {

    func testOnlyTheExpiryOfTheRequestOnScreenShowsTheNextOne() throws {
        struct Row {
            let label: String
            let records: [Record]
            let expiresWhileShown: [Int64]
            let emitted: [Int64]
        }
        let rows = [
            Row(label: "the request on screen expired",
                records: [.session(1, expired: false), .session(2, expired: false)],
                expiresWhileShown: [1], emitted: [1, 2]),
            Row(label: "a request queued behind the one on screen expired",
                records: [.session(1, expired: false), .session(2, expired: false)],
                expiresWhileShown: [2], emitted: [1]),
            Row(label: "an authenticate request expired",
                records: [.session(1, expired: false), .authenticate(2, expired: true)],
                expiresWhileShown: [], emitted: [1]),
            Row(label: "an older request expired before it was shown",
                records: [.session(1, expired: true), .session(2, expired: false)],
                expiresWhileShown: [], emitted: [2]),
            Row(label: "nothing expired",
                records: [.session(1, expired: false)],
                expiresWhileShown: [], emitted: [1])
        ]
        for row in rows {
            let queue = try Queue(row.records)
            let watcher = queue.makeWatcher()
            queue.show()
            for id in row.expiresWhileShown {
                try queue.expire(id)
            }
            queue.advance(5)

            watcher.checkForRequestExpiry()
            queue.runScheduledEmissions(row.label)

            XCTAssertEqual(queue.emitted, row.emitted.map { RPCID($0) }, row.label)
            XCTAssertTrue(queue.pendingIds().allSatisfy { !queue.isExpired($0) }, "\(row.label): expired requests are deleted")
        }
    }

    func testAnsweringAnExpiredRequestShowsTheOneQueuedBehindIt() async throws {
        let queue = try Queue([.session(1, expired: false), .session(2, expired: false)])
        let networking = NetworkingInteractorMock()
        let responder = queue.makeResponder(networking: networking)
        queue.show()
        try queue.expire(1)
        queue.advance(5)

        do {
            try await responder.respondSessionRequest(topic: Queue.topic, requestId: RPCID(1), response: .response(AnyCodable("signature")))
            XCTFail("an expired request is answered with the expired error, not the response")
        } catch SessionResponder.Errors.sessionRequestExpired {}
        queue.runScheduledEmissions("after the expired error")

        XCTAssertEqual(networking.lastErrorCode, SignReasonCode.sessionRequestExpired.code)
        XCTAssertEqual(queue.emitted, [RPCID(1), RPCID(2)])
    }

    func testAnsweringARequestShowsTheOneQueuedBehindItAsBefore() async throws {
        let queue = try Queue([.session(1, expired: false), .session(2, expired: false)])
        let responder = queue.makeResponder(networking: NetworkingInteractorMock())
        queue.show()
        queue.advance(5)

        try await responder.respondSessionRequest(topic: Queue.topic, requestId: RPCID(1), response: .response(AnyCodable("signature")))
        try queue.resolve(1) // the relay resolves the record on respond; the mock doesn't
        queue.runScheduledEmissions("after the answer")

        XCTAssertEqual(queue.emitted, [RPCID(1), RPCID(2)])
    }

    /// The relay took the answer but its acknowledgement never came, so the dapp may have it. The wallet
    /// can't answer again, and the request queued behind it must not wait for its expiry, or for ever
    /// when it has none. The same goes for the expired error Reown sends in place of an answer to a
    /// request that expired on screen.
    func testAnAnswerWhoseAcknowledgementTimesOutShowsTheOneQueuedBehindIt() async throws {
        struct Row {
            let label: String
            let expiresWhileShown: Bool
        }
        let rows = [
            Row(label: "the answer to request 1", expiresWhileShown: false),
            Row(label: "the expired error for request 1", expiresWhileShown: true)
        ]
        for row in rows {
            let queue = try Queue([.session(1, expired: false), .session(2, expired: false)])
            let responder = queue.makeResponder(networking: queue.makeNetworkingWhoseRelayNeverAcknowledges())
            queue.show()
            if row.expiresWhileShown {
                try queue.expire(1)
            }
            queue.advance(5)

            do {
                try await responder.respondSessionRequest(topic: Queue.topic, requestId: RPCID(1), response: .response(AnyCodable("signature")))
                XCTFail("\(row.label): an answer whose acknowledgement times out throws: the wallet can't tell whether the dapp has it")
            } catch RelayError.requestTimeout {
            } catch {
                XCTFail("\(row.label) failed with \(error), not the relay's timeout")
            }
            queue.runScheduledEmissions(row.label)

            XCTAssertEqual(queue.pendingIds(), [RPCID(2)], "\(row.label): request 1 is answered, so it is no longer pending")
            XCTAssertEqual(queue.emitted, [RPCID(1), RPCID(2)], row.label)
        }
    }

    func testAnsweringAnExpiredRequestShowsTheNextEvenIfTheErrorFailsToSend() async throws {
        let queue = try Queue([.session(1, expired: false), .session(2, expired: false)])
        let responder = queue.makeResponder(networking: OfflineNetworking())
        queue.show()
        try queue.expire(1)
        queue.advance(5)

        do {
            try await responder.respondSessionRequest(topic: Queue.topic, requestId: RPCID(1), response: .response(AnyCodable("signature")))
            XCTFail("publishing the expired error fails offline")
        } catch is OfflineNetworking.PublishFailed {}
        queue.runScheduledEmissions("after the failed expired error")

        XCTAssertEqual(queue.emitted, [RPCID(1), RPCID(2)])
    }

    func testAnArrivalDuringTheDelayIsNotShownTwice() throws {
        let queue = try Queue([.session(1, expired: false), .session(2, expired: false)])
        let watcher = queue.makeWatcher()
        queue.show()
        try queue.expire(1)
        queue.advance(5)
        watcher.checkForRequestExpiry()

        // Half a second into the delay a third request arrives, and its emission shows request 2.
        queue.advance(0.5)
        try queue.store(.session(3, expired: false))
        queue.provider.emitRequestIfPending()
        queue.runScheduledEmissions("after the expiry")

        XCTAssertEqual(queue.emitted, [RPCID(1), RPCID(2)], "request 2 is shown once")
    }

    func testAnEmissionThatFindsNothingDoesNotHideTheNextArrival() throws {
        let queue = try Queue([])

        queue.provider.emitRequestIfPending() // nothing pending, as at launch or after the last answer
        queue.advance(0.1)
        try queue.store(.session(1, expired: false))
        queue.provider.emitRequestIfPending() // the arrival's emission, inside the 0.4 s debounce

        XCTAssertEqual(queue.emitted, [RPCID(1)])
    }
}

// MARK: - Helpers

private enum Record {
    case session(Int64, expired: Bool)
    case authenticate(Int64, expired: Bool)
}

private struct FixedTime: TimeProvider {
    let currentDate: Date
}

/// Networking whose error responses fail to publish, as offline.
private final class OfflineNetworking: NetworkingInteractorMock {
    struct PublishFailed: Error {}

    override func respondError(topic: String, requestId: RPCID, protocolMethod: ProtocolMethod, reason: Reason, envelopeType: Envelope.EnvelopeType) async throws {
        throw PublishFailed()
    }
}

/// One pending-request queue: an in-memory `RPCHistory`, its `HistoryService`, and a
/// `SessionRequestsProvider` on a fake clock whose delayed emissions are captured, not scheduled.
private final class Queue {
    static let topic = "topic"
    private static let chain = Blockchain("solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp")!

    let rpcHistory = RPCHistory(keyValueStore: .init(defaults: RuntimeKeyValueStorage(), identifier: ""))
    let historyService: HistoryService
    private(set) var provider: SessionRequestsProvider!
    private(set) var emitted: [RPCID] = []
    private var clock = Date(timeIntervalSince1970: 1_700_000_000)
    private var scheduled: [(due: Date, delay: TimeInterval, work: () -> Void)] = []
    private var storedAt: [Int64: Date] = [:]
    private var subscription: AnyCancellable?

    init(_ records: [Record]) throws {
        historyService = HistoryService(
            history: rpcHistory,
            verifyContextStore: CodableStore<VerifyContext>(defaults: RuntimeKeyValueStorage(), identifier: "")
        )
        provider = SessionRequestsProvider(
            historyService: historyService,
            schedule: { [unowned self] delay, work in
                scheduled.append((clock.addingTimeInterval(delay), delay, work))
            },
            currentDate: { [unowned self] in clock }
        )
        subscription = provider.sessionRequestPublisher.sink { [unowned self] in emitted.append($0.request.id) }
        for record in records {
            try store(record)
        }
    }

    /// Stores a record one second after the previous one, so records are oldest first.
    func store(_ record: Record) throws {
        let id: Int64
        switch record {
        case let .session(recordId, _), let .authenticate(recordId, _):
            id = recordId
        }
        let at = storedAt[id] ?? Date(timeIntervalSince1970: 1_600_000_000 + TimeInterval(storedAt.count))
        storedAt[id] = at
        try rpcHistory.set(try Self.request(record), forTopic: Self.topic, emmitedBy: .remote, time: FixedTime(currentDate: at), transportType: .relay)
    }

    /// The wallet shows the oldest pending request, as on an arrival.
    func show() {
        provider.emitRequestIfPending()
    }

    /// A session request that was valid when shown expires: its record is replaced by an expired copy.
    func expire(_ id: Int64) throws {
        rpcHistory.delete(id: RPCID(id))
        try store(.session(id, expired: true))
    }

    func resolve(_ id: Int64) throws {
        try rpcHistory.resolve(RPCResponse(id: RPCID(id), outcome: .response(AnyCodable("signature"))))
    }

    func advance(_ seconds: TimeInterval) {
        clock = clock.addingTimeInterval(seconds)
    }

    /// Runs every captured emission at the time it was due. Each must be a second out.
    func runScheduledEmissions(_ label: String, file: StaticString = #filePath, line: UInt = #line) {
        while !scheduled.isEmpty {
            let next = scheduled.removeFirst()
            XCTAssertEqual(next.delay, 1, "\(label): the next request is emitted a second later", file: file, line: line)
            clock = max(clock, next.due)
            next.work()
        }
    }

    func pendingIds() -> [RPCID] {
        rpcHistory.getPending().map(\.id)
    }

    func isExpired(_ id: RPCID) -> Bool {
        historyService.getPendingExpirableRequestsWithId().first { $0.1 == id }?.0.isExpired() ?? false
    }

    func makeWatcher() -> RequestsExpiryWatcher {
        RequestsExpiryWatcher(
            proposalPayloadsStore: CodableStore(defaults: RuntimeKeyValueStorage(), identifier: ""),
            rpcHistory: rpcHistory,
            historyService: historyService,
            sessionRequestsProvider: provider,
            startsCheckTimer: false
        )
    }

    /// The real networking and relay client on this queue's history, over a socket that never
    /// acknowledges, so every publish times out.
    func makeNetworkingWhoseRelayNeverAcknowledges() -> NetworkingInteractor {
        networkingWhoseRelayNeverAcknowledges(history: rpcHistory)
    }

    func makeResponder(networking: NetworkInteracting) -> SessionResponder {
        let sessions = WCSessionStorageMock()
        sessions.setSession(WCSession.stub(topic: Self.topic, namespaces: SessionNamespace.stubDictionary()))
        return SessionResponder(
            logger: ConsoleLoggerMock(),
            sessionStore: sessions,
            networkingInteractor: networking,
            verifyContextStore: CodableStore<VerifyContext>(defaults: RuntimeKeyValueStorage(), identifier: ""),
            sessionRequestsProvider: provider,
            historyService: historyService
        )
    }

    private static func request(_ record: Record) throws -> RPCRequest {
        switch record {
        case let .session(id, expired):
            let params = SessionType.RequestParams(
                request: .init(
                    method: "solana_signTransaction",
                    params: AnyCodable(EmptyCodable()),
                    expiryTimestamp: expired ? tenSecondsAgo() : nil
                ),
                chainId: chain
            )
            return RPCRequest(method: SessionRequestProtocolMethod().method, params: params, id: id)
        case let .authenticate(id, expired):
            return RPCRequest(method: "wc_sessionAuthenticate", params: try authenticateParams(expired: expired), id: id)
        }
    }

    /// Expiry is real time (`isExpired` reads `Date()`); only the debounce runs on the fake clock.
    private static func tenSecondsAgo() -> UInt64 {
        UInt64(Date().timeIntervalSince1970) - 10
    }

    /// An authenticate request, with its expiry moved into the past when `expired`: the params set
    /// their expiry from a TTL at init, so it is rewritten through their coding.
    private static func authenticateParams(expired: Bool) throws -> SessionAuthenticateRequestParams {
        let params = SessionAuthenticateRequestParams(
            requester: Participant(publicKey: "", metadata: AppMetadata.stub()),
            authPayload: AuthPayload.stub(),
            ttl: 300
        )
        guard expired else { return params }
        var json = try XCTUnwrap(JSONSerialization.jsonObject(with: JSONEncoder().encode(params)) as? [String: Any])
        json["expiryTimestamp"] = tenSecondsAgo()
        return try JSONDecoder().decode(
            SessionAuthenticateRequestParams.self,
            from: JSONSerialization.data(withJSONObject: json)
        )
    }
}
